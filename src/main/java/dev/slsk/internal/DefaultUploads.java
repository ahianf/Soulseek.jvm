// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.Attachment;
import dev.slsk.EventStream;
import dev.slsk.Priority;
import dev.slsk.TransferId;
import dev.slsk.Upload;
import dev.slsk.Uploads;
import dev.slsk.events.UploadEvent;
import dev.slsk.spi.UploadPolicy;
import dev.slsk.user.Username;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * {@link Uploads}, over the engine.
 *
 * <p>A projection of the engine's upload list, plus the two pieces of state that
 * are ours rather than the engine's: the per-upload priority, and the ban list.
 *
 * <p>Bans live here because of what they change. Banning names a user but
 * decides who <em>we</em> serve, which is upload policy and not a fact about the
 * user, so it belongs to the facet owning that policy. Both ban and unban are
 * idempotent, and the list is a snapshot.
 *
 * <p>The list is held by the admission rather than here, because a ban is
 * checked ahead of the policy and must survive a consumer replacing the policy.
 * That is the difference between a default and an invariant.
 */
final class DefaultUploads implements Uploads {

    private final SoulseekEngine client;
    private final EventBus<UploadEvent> events;
    private final Map<TransferId, Priority> priorities = new ConcurrentHashMap<>();
    private final ProgressCoalescer progress = new ProgressCoalescer(System::nanoTime);

    DefaultUploads(SoulseekEngine client, EventBus<UploadEvent> events) {
        this.client = Objects.requireNonNull(client, "client");
        this.events = Objects.requireNonNull(events, "events");
        // This wiring is what makes the bus speak at all: nothing published an
        // UploadEvent before it, so uploads().events() was silent forever and
        // a finished upload vanished from all() with no Finished ever firing.
        client.transfers().uploadObserver(new TransferDomain.UploadObserver() {
            @Override
            public void stateChanged(dev.slsk.internal.options.TransferStateChange change) {
                onStateChanged(change);
            }

            @Override
            public void progressed(dev.slsk.internal.options.TransferProgressUpdate update) {
                onProgressed(update);
            }
        });
        client.transfers()
                .admission()
                .onDenied((user, path, reason) ->
                        events.publish(new UploadEvent.Denied(user, path, reason, Instant.now())));
    }

    /** Turns an upload transition into the events a consumer sees. */
    private void onStateChanged(dev.slsk.internal.options.TransferStateChange change) {
        Transfer transfer = change.transfer();
        TransferId id = Transfers.id(transfer);
        Instant at = Instant.now();
        if (dev.slsk.internal.TransferState.NONE.equals(change.previousState())) {
            // The first transition is the upload existing at all: a peer asked
            // and the policy accepted.
            events.publish(new UploadEvent.Requested(project(transfer), at));
            return;
        }
        events.publish(new UploadEvent.StateChanged(
                id, Transfers.state(transfer, change.previousState()), Transfers.state(transfer), at));
        if (Transfers.state(transfer) instanceof dev.slsk.TransferState.Finished finished) {
            progress.forget(id);
            events.publish(new UploadEvent.Finished(id, finished.outcome(), at));
        }
    }

    private void onProgressed(dev.slsk.internal.options.TransferProgressUpdate update) {
        Transfer transfer = update.transfer();
        TransferId id = Transfers.id(transfer);
        progress.offer(id, transfer.getBytesTransferred(), transfer.getSize())
                .ifPresent(sample -> events.publish(new UploadEvent.Progressed(id, sample, Instant.now())));
    }

    private Upload project(Transfer transfer) {
        TransferId id = Transfers.id(transfer);
        return new Upload(
                id,
                Username.of(transfer.getUsername()),
                transfer.getFilename(),
                transfer.getSize(),
                Transfers.state(transfer),
                priorities.getOrDefault(id, Priority.NORMAL),
                transfer.getStartTime() == null ? Instant.now() : transfer.getStartTime(),
                Transfers.startedAt(transfer),
                Transfers.endedAt(transfer));
    }

    @Override
    public List<Upload> all() {
        List<Upload> running = client.getUploadRegistry().values().stream()
                .map(dev.slsk.internal.transfer.TransferInternal::toTransfer)
                .map(this::project)
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));

        // Queued requests are uploads too. They were invisible here, which is
        // why prioritize() had nothing it could name: an upload only entered the
        // registry once it started, and by then its place in the queue no longer
        // mattered. TransferState.Queued exists for exactly this — "our own
        // queue, which we control and can reorder".
        List<dev.slsk.internal.transfer.UploadScheduler.Waiting> waiting =
                client.transfers().admission().waiting();
        for (int index = 0; index < waiting.size(); index++) {
            dev.slsk.internal.transfer.UploadScheduler.Waiting pending = waiting.get(index);
            running.add(new Upload(
                    Transfers.uploadId(pending.token()),
                    pending.user(),
                    pending.path(),
                    0,
                    new dev.slsk.TransferState.Queued(index),
                    pending.priority(),
                    Instant.now(),
                    Optional.empty(),
                    Optional.empty()));
        }
        return List.copyOf(running);
    }

    @Override
    public Upload get(TransferId id) {
        return find(id).orElseThrow(() -> new IllegalArgumentException("unknown upload: " + id));
    }

    @Override
    public Optional<Upload> find(TransferId id) {
        Objects.requireNonNull(id, "id");
        return all().stream().filter(upload -> upload.id().equals(id)).findFirst();
    }

    @Override
    public void cancel(TransferId id) {
        Objects.requireNonNull(id, "id");
        client.transfers().cancelUpload(id);
    }

    @Override
    public void prioritize(TransferId id, Priority priority) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(priority, "priority");
        priorities.put(id, priority);
        // Where the promise in the javadoc is actually kept. For a queued
        // upload this moves it in the scheduler's ordering; for one already
        // running it changes nothing, because the slot is taken and this
        // orders work we have not started yet.
        client.transfers().admission().prioritize(id, priority);
    }

    @Override
    public UploadPolicy policy() {
        return client.transfers().uploadPolicy();
    }

    @Override
    public void policy(UploadPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        client.transfers().uploadPolicy(policy);
    }

    @Override
    public void ban(Username user, String reason) {
        client.transfers().admission().ban(user, reason);
    }

    @Override
    public void unban(Username user) {
        client.transfers().admission().unban(user);
    }

    @Override
    public Map<Username, String> banned() {
        return client.transfers().admission().banned();
    }

    @Override
    public EventStream<UploadEvent> events() {
        return events;
    }

    @Override
    public Attachment<List<Upload>> attach(Consumer<UploadEvent> listener) {
        return events.attach(this::all, listener);
    }
}
