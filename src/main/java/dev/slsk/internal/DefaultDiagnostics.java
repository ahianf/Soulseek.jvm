// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.DiagnosticLevel;
import dev.slsk.Diagnostics;
import dev.slsk.EventStream;
import dev.slsk.MeshState;
import dev.slsk.Metrics;
import dev.slsk.Username;
import dev.slsk.events.DiagnosticEvent;
import dev.slsk.events.MeshEvent;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link Diagnostics}, over the engine.
 *
 * <p>The mesh collapse happens here. Seven client listeners — parent adopted,
 * parent disconnected, child added, child disconnected, promoted, demoted,
 * network reset — all resolve to the same question, "where are we in the tree
 * now?", so all seven map onto one handler that reads the current state and
 * publishes it if it differs. A consumer that previously registered seven
 * listeners and reassembled a picture from the fragments registers one and is
 * handed the picture.
 *
 * <p>Publishing only on a real change matters more here than elsewhere: several
 * of those seven fire together for a single mesh event, and without the
 * comparison a consumer would redraw three or four times for one change.
 */
final class DefaultDiagnostics implements Diagnostics {

    private final DefaultSoulseekClient client;
    private final EventBus<DiagnosticEvent> events;
    private final EventBus<MeshEvent> meshEvents;
    private final AtomicReference<MeshState> published = new AtomicReference<>(empty());
    private final AtomicBoolean tracing = new AtomicBoolean();

    DefaultDiagnostics(DefaultSoulseekClient client, EventBus<DiagnosticEvent> events, EventBus<MeshEvent> meshEvents) {
        this.client = Objects.requireNonNull(client, "client");
        this.events = Objects.requireNonNull(events, "events");
        this.meshEvents = Objects.requireNonNull(meshEvents, "meshEvents");
        wire();
    }

    private static MeshState empty() {
        return new MeshState(false, Optional.empty(), List.of(), false, 0, Optional.empty());
    }

    private void wire() {
        client.addDiagnosticGeneratedListener((sender, event) -> {
            if (event == null) {
                return;
            }
            events.publish(new DiagnosticEvent(
                    level(event.getLevel()),
                    event.getMessage(),
                    Optional.ofNullable(event.getException()),
                    event.getTimestamp() == null ? Instant.now() : event.getTimestamp()));
        });

        client.addDistributedParentAdoptedListener((sender, event) -> onMeshChanged());
        client.addDistributedParentDisconnectedListener((sender, event) -> onMeshChanged());
        client.addDistributedChildAddedListener((sender, event) -> onMeshChanged());
        client.addDistributedChildDisconnectedListener((sender, event) -> onMeshChanged());
        client.addPromotedToDistributedBranchRootListener((sender, event) -> onMeshChanged());
        client.addDemotedFromDistributedBranchRootListener((sender, event) -> onMeshChanged());
        client.addDistributedNetworkResetListener((sender, event) -> onMeshChanged());
    }

    private void onMeshChanged() {
        meshEvents.mutateAndPublish(() -> {
            MeshState current = mesh();
            MeshState previous = published.getAndSet(current);
            return previous.equals(current) ? null : new MeshEvent.StateChanged(previous, current, Instant.now());
        });
    }

    private static DiagnosticLevel level(dev.slsk.internal.diagnostics.DiagnosticLevel source) {
        if (source == null) {
            return DiagnosticLevel.INFO;
        }
        return switch (source) {
            case NONE -> DiagnosticLevel.NONE;
            case WARNING -> DiagnosticLevel.WARNING;
            case INFO -> DiagnosticLevel.INFO;
            case DEBUG -> DiagnosticLevel.DEBUG;
            case TRACE -> DiagnosticLevel.TRACE;
        };
    }

    @Override
    public EventStream<DiagnosticEvent> events() {
        return events;
    }

    @Override
    public Metrics metrics() {
        // Wired to real counters when the queues land; the shape is what
        // consumers bind to and it is stable now.
        return Metrics.empty();
    }

    @Override
    public MeshState mesh() {
        DistributedNetworkInfo info = client.getDistributedNetwork();
        if (info == null) {
            return empty();
        }
        DistributedPeer parent = info.getParent();
        List<Username> children = info.getChildren() == null
                ? List.of()
                : info.getChildren().stream()
                        .filter(child -> child != null && isName(child.username()))
                        .map(child -> Username.of(child.username()))
                        .toList();
        return new MeshState(
                info.hasParent(),
                user(parent == null ? null : parent.username()),
                children,
                info.isBranchRoot(),
                info.getBranchLevel(),
                user(info.getBranchRoot()));
    }

    /**
     * The internal mesh types spell "nobody" as an empty string in some places
     * and {@code null} in others — {@code getBranchRoot()} returns {@code ""}
     * when there is no branch root. Both mean absent, and neither is a username.
     */
    private static Optional<Username> user(String value) {
        return isName(value) ? Optional.of(Username.of(value)) : Optional.empty();
    }

    private static boolean isName(String value) {
        return value != null && !value.isBlank();
    }

    @Override
    public EventStream<MeshEvent> meshEvents() {
        return meshEvents;
    }

    @Override
    public void protocolTrace(boolean enabled) {
        tracing.set(enabled);
    }
}
