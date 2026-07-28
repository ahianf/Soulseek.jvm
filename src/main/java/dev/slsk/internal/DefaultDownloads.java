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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
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

    private final DefaultSoulseekClient client;
    private final EventBus<DownloadEvent> events;

    /** Per-enqueue data the engine does not carry. */
    private final Map<TransferId, Metadata> metadata = new ConcurrentHashMap<>();

    DefaultDownloads(DefaultSoulseekClient client, EventBus<DownloadEvent> events) {
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
        dev.slsk.internal.DownloadRequest internal = dev.slsk.internal.DownloadRequest.toFile(
                        request.user().value(),
                        request.path(),
                        request.destination().toString())
                .size(request.expectedSize() == 0 ? null : request.expectedSize())
                .token(token)
                .cancellation(cancellation.getSignal())
                .build();
        TransferId id = TransferId.of("DOWNLOAD:" + token);
        metadata.put(id, new Metadata(request.priority(), request.tags(), cancellation));
        client.enqueueDownload(internal);
        events.publish(new DownloadEvent.Enqueued(get(id), java.time.Instant.now()));
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
        List<Transfer> transfers = client.getDownloads();
        return transfers == null
                ? List.of()
                : transfers.stream().map(this::project).toList();
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
