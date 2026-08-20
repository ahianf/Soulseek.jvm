// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.Attachment;
import dev.slsk.Downloads;
import dev.slsk.EventStream;
import dev.slsk.download.Download;
import dev.slsk.download.DownloadPolicy;
import dev.slsk.download.DownloadRequest;
import dev.slsk.events.DownloadEvent;
import dev.slsk.exceptions.TransferNotFoundException;
import dev.slsk.internal.common.Usernames;
import dev.slsk.internal.concurrent.BlockingInvocation;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.concurrent.InterruptibleWaits;
import dev.slsk.internal.events.EventBus;
import dev.slsk.internal.messaging.handlers.PeerServices;
import dev.slsk.internal.messaging.messages.TransferRequest;
import dev.slsk.internal.options.TransferOptions;
import dev.slsk.internal.transfer.Transfer;
import dev.slsk.spi.TransferStore;
import dev.slsk.transfer.Priority;
import dev.slsk.transfer.TransferId;
import dev.slsk.transfer.TransferOutcome;
import dev.slsk.transfer.TransferState;
import dev.slsk.user.Username;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * {@link Downloads}, over the managed queue.
 *
 * <p>Enqueueing no longer starts anything. The request goes into {@link
 * DownloadQueue}, which decides when it runs and against which peer, and this
 * class turns the queue's decisions into events and snapshots. That inversion is
 * most of what 1.0 adds here: before it, "enqueue" meant "ask the peer now, and
 * mind the consequences yourself".
 *
 * <p>The sink is opened when the peer accepts rather than when the download is
 * queued, so a hundred queued downloads are a hundred records rather than a
 * hundred open files, and one that is refused leaves nothing behind.
 */
final class DefaultDownloads implements Downloads {

    private final SoulseekEngine client;
    private final EventBus<DownloadEvent> events;
    private final DownloadQueue queue;
    private final ProgressCoalescer progress = new ProgressCoalescer(System::nanoTime);

    /**
     * Offers a peer has made for downloads that had not started yet.
     *
     * <p>Handed to the attempt rather than signalled to it. The engine's wait
     * for a peer's transfer request is registered part-way through the download,
     * so completing it from here would race the very attempt it is meant to
     * drive — and a wait that is completed before it is registered is dropped
     * silently. Data does not have that problem: the attempt reads its offer
     * whenever it gets there.
     */
    private final Map<PeerFile, TransferRequest> offers = new ConcurrentHashMap<>();

    /**
     * Takes up a peer's offer of a file we have queued but not started.
     *
     * @param username who is offering
     * @param filename what they are offering
     * @param offer their request
     * @return what became of the offer
     */
    PeerServices.OfferDisposition offered(String username, String filename, TransferRequest offer) {
        Username user = Usernames.fromWire(username);
        if (user == null) {
            // Peer-supplied; nothing we queued can be keyed by an
            // unrepresentable name, so the offer cannot be one of ours.
            return PeerServices.OfferDisposition.UNKNOWN;
        }
        PeerFile file = new PeerFile(user, filename);
        offers.put(file, offer);
        if (queue.promote(user, filename).isPresent()) {
            return PeerServices.OfferDisposition.TAKEN;
        }
        offers.remove(file);
        return queue.isComplete(user, filename)
                ? PeerServices.OfferDisposition.COMPLETE
                : PeerServices.OfferDisposition.UNKNOWN;
    }

    DefaultDownloads(SoulseekEngine client, EventBus<DownloadEvent> events) {
        this(client, events, TransferStore.inMemory());
    }

    DefaultDownloads(SoulseekEngine client, EventBus<DownloadEvent> events, TransferStore store) {
        this.client = Objects.requireNonNull(client, "client");
        this.events = Objects.requireNonNull(events, "events");
        this.queue = new DownloadQueue(client.getScheduler(), this::fetch, store, this::publish);
        this.queue.onRetryScheduled((entry, nextAttemptAt) -> events.publish(
                new DownloadEvent.RetryScheduled(entry.id(), entry.attempt() + 1, nextAttemptAt, Instant.now())));
        this.queue.onPositionChanged((entry, place) ->
                events.publish(new DownloadEvent.QueuePositionChanged(entry.id(), place, Instant.now())));
        this.queue.positionProbe(this::place);
        client.transfers().downloadOffers(this::offered);
        client.transfers().downloadPositions(this::positionReported);
        applyConcurrency(queue.policy());
    }

    /**
     * Hands the transfer path the policy's concurrency ceilings.
     *
     * <p>The same division as the rate ceiling below: the queue decides which
     * downloads exist and how many each peer is asked to hold, and these bound
     * how many may be moving bytes at once. They are applied there rather than
     * here because the moment they bind is the moment a peer says it is ready,
     * which is inside the transfer, not inside the queue.
     */
    private void applyConcurrency(DownloadPolicy policy) {
        client.transfers().downloadConcurrency(policy.maxConcurrent(), policy.maxConcurrentPerUser());
    }

    /**
     * Runs one attempt against a peer.
     *
     * <p>A direct call, and the whole of what this method is. It used to
     * translate a queue entry into the pre-1.0 transfer path, correlate the
     * engine's progress and state broadcasts back to this entry through a token
     * map, and infer an outcome from all of it. The transfer path speaks the
     * queue's language now: it takes a request, blocks, and says how it ended.
     */
    private TransferOutcome fetch(DownloadQueue.Entry entry) {
        DownloadRequest request = entry.request();
        TransferId id = entry.id();
        AtomicReference<SinkOutputStream> stream = new AtomicReference<>();
        // What the last attempt left on disk. The peer is asked to start there
        // and the sink is opened there, so a download interrupted at ninety
        // percent costs ten percent to finish rather than another whole file.
        long resumeFrom = entry.resumeOffset();
        dev.slsk.internal.transfer.DownloadSpecification internal =
                dev.slsk.internal.transfer.DownloadSpecification.toStream(
                                request.user().value(), request.path(), () -> {
                                    try {
                                        SinkOutputStream opened = new SinkOutputStream(request.sink(), resumeFrom);
                                        stream.set(opened);
                                        return opened;
                                    } catch (IOException failure) {
                                        // The transfer path treats a stream it cannot
                                        // open as a failed transfer, which is what this
                                        // is; there is no separate "could not start".
                                        throw new dev.slsk.exceptions.TransferStreamException(
                                                "Failed to open the transfer sink", failure);
                                    }
                                })
                        .size(request.expectedSize() == 0 ? null : request.expectedSize())
                        .startOffset(resumeFrom)
                        .token(client.getNextToken())
                        // Present only when this attempt exists because a peer offered
                        // the file; the transfer then skips asking for what it has
                        // already been given.
                        .offer(offers.remove(new PeerFile(request.user(), request.path())))
                        .cancellation(entry.signal())
                        .options(TransferOptions.builder()
                                .stateChanged(change -> observed(id, change.transfer()))
                                .progressUpdated(update -> progressed(id, update.transfer()))
                                .build())
                        .build();

        try {
            TransferOutcome outcome = client.transfers().download(internal);
            settle(stream.get(), outcome);
            return outcome;
        } catch (RuntimeException refused) {
            // A request that never became a transfer: a duplicate, or a client
            // that is not logged in. The transfer path reports everything that
            // happened to a transfer as an outcome.
            TransferOutcome outcome = outcomeOf(refused);
            settle(stream.get(), outcome);
            return outcome;
        } finally {
            progress.forget(id);
            SinkOutputStream opened = stream.get();
            entry.resumeOffset(opened == null ? resumeFrom : opened.getPosition());
        }
    }

    /**
     * Takes a place-in-queue a peer reported, whether we asked or not.
     *
     * @param username who sent it
     * @param filename the file it is about
     * @param position where that peer says we are
     */
    private void positionReported(String username, String filename, int position) {
        Username user = Usernames.fromWire(username);
        if (user == null) {
            // Peer-supplied; nothing we queued can be keyed by an
            // unrepresentable name.
            return;
        }
        queue.positionReported(user, filename, position);
    }

    /**
     * Asks a peer where we are in its queue.
     *
     * <p>The old surface made this the consumer's job, which meant every
     * application wrote a timer, a concurrency guard and a "did it change?"
     * comparison — {@code tenine} had a hundred and fourteen lines of it. The
     * library polls because the library already knows which downloads are
     * remotely queued and which peer to ask.
     */
    private java.util.OptionalInt place(DownloadQueue.Entry entry) {
        try {
            Integer position = client.transfers()
                    .getDownloadPlaceInQueue(
                            entry.user().value(), entry.request().path());
            return position == null ? java.util.OptionalInt.empty() : java.util.OptionalInt.of(position);
        } catch (InterruptedException interrupted) {
            // A poll cut short answers nothing; the position we have is still
            // the last one the peer gave.
            Thread.currentThread().interrupt();
            return java.util.OptionalInt.empty();
        } catch (RuntimeException unreachable) {
            return java.util.OptionalInt.empty();
        }
    }

    /**
     * Records what the transfer says it is doing.
     *
     * <p>The queue owns queued, paused and finished; everything between them
     * belongs to the transfer, and without this a download would read as
     * {@code Requesting} from the moment it was admitted to the moment it ended.
     */
    private void observed(TransferId id, Transfer transfer) {
        queue.observed(id, Transfers.state(transfer));
    }

    /**
     * Publishes progress on a fixed cadence, with the rate smoothed.
     *
     * <p>The transfer reports one of these per socket read. Most of them are
     * dropped here, which is the point: the event rate a consumer sees is
     * bounded by the library rather than by how fast the network happens to be.
     */
    private void progressed(TransferId id, Transfer transfer) {
        progress.offer(id, transfer.bytesTransferred(), transfer.size()).ifPresent(value -> {
            // Both, and neither is redundant. The event is for a consumer that
            // subscribes; the queue update is for one that polls `all()`, which
            // would otherwise read a byte count frozen at the moment the
            // transfer started.
            queue.progressed(id, value);
            events.publish(new DownloadEvent.Progressed(id, value, Instant.now()));
        });
    }

    /** Classifies a thrown failure the way the transfer path would have. */
    private static TransferOutcome outcomeOf(RuntimeException failure) {
        if (failure instanceof java.util.concurrent.CancellationException) {
            return new TransferOutcome.Cancelled();
        }
        if (failure instanceof dev.slsk.exceptions.TransferRejectedException rejected) {
            String message = rejected.getMessage() == null ? "" : rejected.getMessage();
            return new TransferOutcome.Rejected(RejectionReasons.parse(message), message);
        }
        return new TransferOutcome.Failed(failure, true);
    }

    /**
     * Tells the sink how the attempt ended.
     *
     * <p>Commit exactly once on success, discard on anything else. This is the
     * whole of what the sink contract buys: an application no longer has to work
     * out for itself whether a transfer that stopped is one whose bytes are safe
     * to publish.
     */
    private void settle(SinkOutputStream stream, TransferOutcome outcome) {
        if (stream == null) {
            return;
        }
        if (outcome instanceof TransferOutcome.Succeeded) {
            try {
                stream.commit();
                return;
            } catch (IOException failure) {
                client.getDiagnostic().warning("Failed to commit a completed download", failure);
            }
        }
        stream.discard();
    }

    /** Turns a queue transition into the events a consumer sees. */
    private void publish(DownloadQueue.Entry entry, TransferState previous) {
        Download snapshot = entry.snapshot();
        Instant at = Instant.now();
        events.publish(new DownloadEvent.StateChanged(entry.id(), previous, snapshot.state(), at));
        if (snapshot.state() instanceof TransferState.Finished finished) {
            events.publish(new DownloadEvent.Finished(entry.id(), finished.outcome(), at));
        }
    }

    @Override
    public TransferId enqueue(DownloadRequest request) {
        Objects.requireNonNull(request, "request");
        // The id exists before anything is asked of a peer, because a consumer
        // is handed it synchronously and has to be able to cancel what it just
        // queued.
        TransferId id = TransferId.of("DOWNLOAD:" + client.getNextToken());
        DownloadQueue.Entry entry = queue.enqueue(id, request);
        events.publish(new DownloadEvent.Enqueued(entry.snapshot(), Instant.now()));
        return id;
    }

    @Override
    public List<TransferId> enqueueAll(List<DownloadRequest> requests) {
        Objects.requireNonNull(requests, "requests");
        List<TransferId> ids = new ArrayList<>(requests.size());
        for (DownloadRequest request : requests) {
            ids.add(enqueue(request));
        }
        return List.copyOf(ids);
    }

    @Override
    public void pause(TransferId id) {
        queue.pause(id);
    }

    @Override
    public void resume(TransferId id) {
        queue.resume(id);
    }

    @Override
    public void cancel(TransferId id) {
        queue.cancel(id);
    }

    @Override
    public void retry(TransferId id) {
        queue.retry(id);
    }

    @Override
    public void forget(TransferId id) {
        if (queue.forget(id)) {
            events.publish(new DownloadEvent.Forgotten(id, Instant.now()));
        }
    }

    @Override
    public void prioritize(TransferId id, Priority priority) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(priority, "priority");
        queue.prioritize(id, priority);
    }

    @Override
    public Download await(TransferId id) throws InterruptedException {
        return BlockingInvocation.run(signal -> await(id, signal));
    }

    @Override
    public Download await(TransferId id, Duration timeout) throws InterruptedException, TimeoutException {
        return BlockingInvocation.run(client.getScheduler(), timeout, signal -> await(id, signal));
    }

    private Download await(TransferId id, CancellationSignal signal) throws InterruptedException {
        Download current = get(id);
        if (current.state() instanceof TransferState.Finished) {
            return current;
        }

        CountDownLatch finished = new CountDownLatch(1);
        try (dev.slsk.Subscription subscription = events.subscribe(DownloadEvent.Finished.class, event -> {
                    if (event.id().equals(id)) {
                        finished.countDown();
                    }
                });
                dev.slsk.internal.concurrent.CancellationSubscription cancelled =
                        signal.register(finished::countDown)) {
            // Re-read after subscribing: it may have finished between the two,
            // and a wait that misses its own event never returns.
            if (!(get(id).state() instanceof TransferState.Finished)) {
                InterruptibleWaits.await(finished, () -> get(id).state() instanceof TransferState.Finished);
            }
        }
        signal.throwIfCancellationRequested();
        return get(id);
    }

    @Override
    public DownloadPolicy policy() {
        return queue.policy();
    }

    @Override
    public void policy(DownloadPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        // The rate ceiling is the engine's to apply: the queue decides what runs
        // and the token bucket decides how fast, and a policy that carried a
        // limit nothing read would be a setting that silently did nothing.
        client.setDownloadSpeedLimit(policy.speedLimit());
        applyConcurrency(policy);
        queue.policy(policy);
    }

    @Override
    public Download get(TransferId id) {
        return find(id).orElseThrow(() -> new TransferNotFoundException("unknown download: " + id));
    }

    @Override
    public Optional<Download> find(TransferId id) {
        Objects.requireNonNull(id, "id");
        return queue.find(id).map(DownloadQueue.Entry::snapshot);
    }

    @Override
    public List<Download> all() {
        return queue.all();
    }

    @Override
    public EventStream<DownloadEvent> events() {
        return events;
    }

    @Override
    public Attachment<List<Download>> attach(Consumer<DownloadEvent> listener) {
        return events.attach(this::all, listener);
    }

    /** Cancels everything still in flight. Called when the client closes. */
    void close() {
        queue.close();
    }
}
