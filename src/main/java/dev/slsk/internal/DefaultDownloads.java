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
import dev.slsk.events.DownloadEvent;
import dev.slsk.exceptions.TransferNotFoundException;
import dev.slsk.internal.common.Blocking;
import dev.slsk.spi.TransferStore;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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

    DefaultDownloads(SoulseekEngine client, EventBus<DownloadEvent> events) {
        this(client, events, TransferStore.inMemory());
    }

    DefaultDownloads(SoulseekEngine client, EventBus<DownloadEvent> events, TransferStore store) {
        this.client = Objects.requireNonNull(client, "client");
        this.events = Objects.requireNonNull(events, "events");
        this.queue = new DownloadQueue(client.getScheduler(), this::fetch, store, this::publish);
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
        dev.slsk.internal.DownloadRequest internal = dev.slsk.internal.DownloadRequest.toStream(
                        request.user().value(), request.path(), () -> {
                            try {
                                SinkOutputStream opened = new SinkOutputStream(request.sink(), 0);
                                stream.set(opened);
                                return java.util.concurrent.CompletableFuture.completedFuture(opened);
                            } catch (IOException failure) {
                                return java.util.concurrent.CompletableFuture.failedFuture(failure);
                            }
                        })
                .size(request.expectedSize() == 0 ? null : request.expectedSize())
                .token(token)
                .cancellation(entry.signal())
                .build();

        try {
            Transfer completed = Blocking.await(client.transfers().download(internal));
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
        }
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
