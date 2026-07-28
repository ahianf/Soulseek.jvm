// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.Attachment;
import dev.slsk.CancellationController;
import dev.slsk.Download;
import dev.slsk.DownloadRequest;
import dev.slsk.Downloads;
import dev.slsk.EventStream;
import dev.slsk.Priority;
import dev.slsk.TransferId;
import dev.slsk.Username;
import dev.slsk.events.DownloadEvent;
import dev.slsk.internal.common.Blocking;
import dev.slsk.internal.options.TransferOptions;
import dev.slsk.internal.options.TransferStateChange;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * {@link Downloads}, over the engine.
 *
 * <p>A projection of the existing transfer engine, plus the per-enqueue data the
 * engine has no place for: the priority and the application's tags, which are
 * keyed on {@link TransferId} here because the engine knows only {@code (user,
 * filename)} and would alias a re-download onto the record of the one before it.
 *
 * <p>Pause, resume, retry and await are not here. They need the scheduling that
 * arrives with the managed queue, and a method that is declared before it works
 * only defers the discovery that it does not.
 */
final class DefaultDownloads implements Downloads {

    private final SoulseekEngine client;
    private final EventBus<DownloadEvent> events;

    /** Per-enqueue data the engine does not carry. */
    private final Map<TransferId, Metadata> metadata = new ConcurrentHashMap<>();

    DefaultDownloads(SoulseekEngine client, EventBus<DownloadEvent> events) {
        this.client = Objects.requireNonNull(client, "client");
        this.events = Objects.requireNonNull(events, "events");
    }

    /**
     * Per-enqueue data, including the controller that makes cancel work. The
     * engine cancels through a signal supplied at enqueue time, so holding the
     * controller is the only way a later cancel(id) can reach the transfer.
     */
    private record Metadata(Priority priority, Map<String, String> tags, CancellationController cancellation) {}

    private Download project(Transfer transfer) {
        TransferId id = Transfers.id(transfer);
        Metadata data = metadata.get(id);
        Priority priority = data == null ? Priority.NORMAL : data.priority();
        Map<String, String> tags = data == null ? Map.of() : data.tags();
        return new Download(
                id,
                Username.of(transfer.getUsername()),
                transfer.getFilename(),
                transfer.getSize(),
                Transfers.state(transfer),
                priority,
                transfer.getStartTime() == null ? java.time.Instant.now() : transfer.getStartTime(),
                Transfers.startedAt(transfer),
                Transfers.endedAt(transfer),
                1,
                tags);
    }

    @Override
    public TransferId enqueue(DownloadRequest request) {
        Objects.requireNonNull(request, "request");
        // The token is chosen here rather than left to the engine, because the
        // id has to exist before enqueue returns: TransferHandle carries no
        // token, and a consumer cannot be handed an id derived from a transfer
        // that has not appeared yet.
        int token = client.getNextToken();
        CancellationController cancellation = new CancellationController();
        // One sink per attempt, opened lazily: the engine decides when the peer
        // has accepted, and opening a file for a download that is refused would
        // leave a part file for a transfer that never started.
        AtomicReference<SinkOutputStream> stream = new AtomicReference<>();
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
                .options(new TransferOptions().withAdditionalStateChanged(change -> settle(stream, change)))
                .cancellation(cancellation.getSignal())
                .build();
        TransferId id = TransferId.of("DOWNLOAD:" + token);
        metadata.put(id, new Metadata(request.priority(), request.tags(), cancellation));
        Blocking.await(client.transfers().enqueueDownload(internal));
        events.publish(new DownloadEvent.Enqueued(get(id), java.time.Instant.now()));
        return id;
    }

    /**
     * Tells the sink how the transfer ended.
     *
     * <p>Commit exactly once on success, discard on anything else. This is the
     * whole of what the sink contract buys: an application that writes to a file
     * no longer has to work out for itself whether a transfer that stopped is
     * one whose bytes are safe to publish.
     */
    private void settle(AtomicReference<SinkOutputStream> stream, TransferStateChange change) {
        dev.slsk.internal.TransferState state = change.transfer().getState();
        if (!state.contains(dev.slsk.internal.TransferState.COMPLETED)) {
            return;
        }
        SinkOutputStream opened = stream.getAndSet(null);
        if (opened == null) {
            return;
        }
        if (state.contains(dev.slsk.internal.TransferState.SUCCEEDED)) {
            try {
                opened.commit();
                return;
            } catch (IOException failure) {
                client.getDiagnostic().warning("Failed to commit a completed download", failure);
            }
        }
        opened.discard();
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

    /**
     * Cancels through the controller supplied at enqueue time. Idempotent, and
     * cancelling an unknown or finished download does nothing rather than
     * throwing -- the consumer is usually an HTTP handler that cannot know
     * whether its previous request arrived.
     */
    @Override
    public void cancel(TransferId id) {
        Objects.requireNonNull(id, "id");
        Metadata data = metadata.get(id);
        if (data != null) {
            data.cancellation().cancel();
        }
    }

    @Override
    public void forget(TransferId id) {
        Objects.requireNonNull(id, "id");
        Download download = find(id).orElse(null);
        if (download == null) {
            return;
        }
        if (!download.isFinished()) {
            throw new IllegalStateException("cannot forget a download that has not finished: " + id);
        }
        events.mutateAndPublish(() -> {
            metadata.remove(id);
            return new DownloadEvent.Forgotten(id, java.time.Instant.now());
        });
    }

    @Override
    public void prioritize(TransferId id, Priority priority) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(priority, "priority");
        metadata.computeIfPresent(id, (key, data) -> new Metadata(priority, data.tags(), data.cancellation()));
    }

    @Override
    public Download get(TransferId id) {
        return find(id).orElseThrow(() -> new IllegalArgumentException("unknown download: " + id));
    }

    @Override
    public Optional<Download> find(TransferId id) {
        Objects.requireNonNull(id, "id");
        return all().stream().filter(download -> download.id().equals(id)).findFirst();
    }

    @Override
    public List<Download> all() {
        return client.getDownloadRegistry().values().stream()
                .map(dev.slsk.internal.transfer.TransferInternal::toTransfer)
                .map(this::project)
                .toList();
    }

    @Override
    public EventStream<DownloadEvent> events() {
        return events;
    }

    @Override
    public Attachment<List<Download>> attach(Consumer<DownloadEvent> listener) {
        return events.attach(this::all, listener);
    }
}
