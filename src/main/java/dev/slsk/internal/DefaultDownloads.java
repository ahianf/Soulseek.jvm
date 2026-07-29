// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.Attachment;
import dev.slsk.CancellationSignal;
import dev.slsk.Download;
import dev.slsk.DownloadPolicy;
import dev.slsk.DownloadRequest;
import dev.slsk.Downloads;
import dev.slsk.EventStream;
import dev.slsk.Priority;
import dev.slsk.TransferId;
import dev.slsk.TransferOutcome;
import dev.slsk.TransferState;
import dev.slsk.Username;
import dev.slsk.events.DownloadEvent;
import dev.slsk.exceptions.TransferNotFoundException;
import dev.slsk.internal.messaging.handlers.PeerServices;
import dev.slsk.internal.messaging.messages.TransferRequest;
import dev.slsk.spi.TransferStore;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
     * Which queue entry a running transfer belongs to.
     *
     * <p>The engine knows transfers by the token it was given; the queue knows
     * them by the id a consumer holds. They are different because a retry is the
     * same download and a different transfer, which is exactly the distinction
     * the old surface could not make.
     */
    private final Map<Integer, TransferId> running = new ConcurrentHashMap<>();

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
        Username user = Username.of(username);
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
        client.events().on(EngineEvents.Kind.TRANSFER_PROGRESS_UPDATED, this::onProgress);
        client.events().on(EngineEvents.Kind.TRANSFER_STATE_CHANGED, this::onTransferState);
    }

    /**
     * Runs one attempt against a peer.
     *
     * <p>Everything below here is the pre-1.0 transfer path, unchanged: the
     * queue decides <em>whether</em> to run this, and the engine still knows
     * <em>how</em>.
     */
    private TransferOutcome fetch(DownloadQueue.Entry entry) {
        DownloadRequest request = entry.request();
        AtomicReference<SinkOutputStream> stream = new AtomicReference<>();
        int token = client.getNextToken();
        // What the last attempt left on disk. The peer is asked to start there
        // and the sink is opened there, so a download interrupted at ninety
        // percent costs ten percent to finish rather than another whole file.
        long resumeFrom = entry.resumeOffset();
        dev.slsk.internal.DownloadRequest internal = dev.slsk.internal.DownloadRequest.toStream(
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
                .token(token)
                // Present only when this attempt exists because a peer offered
                // the file; the transfer then skips asking for what it has
                // already been given.
                .offer(offers.remove(new PeerFile(request.user(), request.path())))
                .cancellation(entry.signal())
                .build();

        running.put(token, entry.id());
        try {
            Transfer completed = client.transfers().download(internal);
            TransferState state = Transfers.state(completed);
            TransferOutcome outcome = state instanceof TransferState.Finished finished
                    ? finished.outcome()
                    : new TransferOutcome.Succeeded(completed.getBytesTransferred(), java.time.Duration.ZERO);
            settle(stream.get(), outcome);
            return outcome;
        } catch (RuntimeException failure) {
            TransferOutcome outcome = outcomeOf(failure);
            settle(stream.get(), outcome);
            return outcome;
        } finally {
            running.remove(token);
            progress.forget(entry.id());
            SinkOutputStream opened = stream.get();
            entry.resumeOffset(opened == null ? resumeFrom : opened.getPosition());
        }
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
        } catch (RuntimeException unreachable) {
            return java.util.OptionalInt.empty();
        }
    }

    /**
     * Records what the engine says a running transfer is doing.
     *
     * <p>The queue owns queued, paused and finished; everything between them
     * belongs to the transfer, and without this a download would read as
     * {@code Requesting} from the moment it was admitted to the moment it ended.
     */
    private void onTransferState(dev.slsk.internal.events.TransferStateChangedEvent event) {
        if (event == null || event.getTransfer() == null) {
            return;
        }
        Transfer transfer = event.getTransfer();
        TransferId id = running.get(transfer.getToken());
        if (id != null) {
            queue.observed(id, Transfers.state(transfer));
        }
    }

    /**
     * Publishes progress on a fixed cadence, with the rate smoothed.
     *
     * <p>The engine raises one of these per socket read. Most of them are
     * dropped here, which is the point: the event rate a consumer sees is
     * bounded by the library rather than by how fast the network happens to be.
     */
    private void onProgress(dev.slsk.internal.events.TransferProgressUpdatedEvent event) {
        if (event == null || event.getTransfer() == null) {
            return;
        }
        Transfer transfer = event.getTransfer();
        TransferId id = running.get(transfer.getToken());
        if (id == null) {
            return;
        }
        progress.offer(id, transfer.getBytesTransferred(), transfer.getSize()).ifPresent(value -> {
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
    public Download await(TransferId id, CancellationSignal signal) {
        Objects.requireNonNull(signal, "signal");
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
                dev.slsk.CancellationSubscription cancelled = signal.register(finished::countDown)) {
            // Re-read after subscribing: it may have finished between the two,
            // and a wait that misses its own event never returns.
            if (!(get(id).state() instanceof TransferState.Finished)) {
                waitFor(finished);
            }
        }
        signal.throwIfCancellationRequested();
        return get(id);
    }

    private static void waitFor(CountDownLatch latch) {
        try {
            latch.await(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new java.util.concurrent.CancellationException("the wait was interrupted");
        }
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
