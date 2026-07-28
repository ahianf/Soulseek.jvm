// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.Attachment;
import dev.slsk.EventStream;
import dev.slsk.Priority;
import dev.slsk.TransferId;
import dev.slsk.Upload;
import dev.slsk.Uploads;
import dev.slsk.Username;
import dev.slsk.events.UploadEvent;
import dev.slsk.spi.UploadPolicy;
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

    DefaultUploads(SoulseekEngine client, EventBus<UploadEvent> events) {
        this.client = Objects.requireNonNull(client, "client");
        this.events = Objects.requireNonNull(events, "events");
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
        return client.getUploadRegistry().values().stream()
                .map(dev.slsk.internal.transfer.TransferInternal::toTransfer)
                .map(this::project)
                .toList();
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
        client.cancelUpload(id);
    }

    @Override
    public void prioritize(TransferId id, Priority priority) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(priority, "priority");
        priorities.put(id, priority);
    }

    @Override
    public UploadPolicy policy() {
        return client.getUploadPolicy();
    }

    @Override
    public void policy(UploadPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        client.setUploadPolicy(policy);
    }

    @Override
    public void ban(Username user, String reason) {
        client.getUploadAdmission().ban(user, reason);
    }

    @Override
    public void unban(Username user) {
        client.getUploadAdmission().unban(user);
    }

    @Override
    public Map<Username, String> banned() {
        return client.getUploadAdmission().banned();
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
