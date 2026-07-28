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
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.events.DistributedChildEvent;
import dev.slsk.internal.events.DistributedParentEvent;
import dev.slsk.internal.network.DistributedConnectionManager;
import dev.slsk.internal.network.PeerEndpoint;
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

    private final DistributedConnectionManager mesh;
    private final EventBus<DiagnosticEvent> events;
    private final EventBus<MeshEvent> meshEvents;
    private final AtomicReference<MeshState> published = new AtomicReference<>(empty());
    private final AtomicBoolean tracing = new AtomicBoolean();

    DefaultDiagnostics(SoulseekEngine client, EventBus<DiagnosticEvent> events, EventBus<MeshEvent> meshEvents) {
        this.mesh = Objects.requireNonNull(client, "client").getDistributedConnectionManager();
        this.events = Objects.requireNonNull(events, "events");
        this.meshEvents = Objects.requireNonNull(meshEvents, "meshEvents");
        wire(client);
    }

    private static MeshState empty() {
        return new MeshState(false, Optional.empty(), List.of(), false, 0, Optional.empty());
    }

    private void wire(SoulseekEngine client) {
        client.events().on(Kind.DIAGNOSTIC_GENERATED, this::onDiagnostic);

        client.events().on(Kind.DISTRIBUTED_PARENT_ADOPTED, (DistributedParentEvent event) -> onMeshChanged());
        client.events().on(Kind.DISTRIBUTED_PARENT_DISCONNECTED, (DistributedParentEvent event) -> onMeshChanged());
        client.events().on(Kind.DISTRIBUTED_CHILD_ADDED, (DistributedChildEvent event) -> onMeshChanged());
        client.events().on(Kind.DISTRIBUTED_CHILD_DISCONNECTED, (DistributedChildEvent event) -> onMeshChanged());
        client.events().on(Kind.PROMOTED_TO_DISTRIBUTED_BRANCH_ROOT, (Void event) -> onMeshChanged());
        client.events().on(Kind.DEMOTED_FROM_DISTRIBUTED_BRANCH_ROOT, (Void event) -> onMeshChanged());
        client.events().on(Kind.DISTRIBUTED_NETWORK_RESET, (Void event) -> onMeshChanged());
    }

    /** The internal record carries boxed nulls; the published one does not. */
    private void onDiagnostic(dev.slsk.internal.diagnostics.DiagnosticEvent event) {
        if (event == null) {
            return;
        }
        events.publish(new DiagnosticEvent(
                level(event.getLevel()),
                event.getMessage(),
                Optional.ofNullable(event.getException()),
                event.getTimestamp() == null ? Instant.now() : event.getTimestamp()));
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

    /**
     * Reads the mesh straight off the connection manager.
     *
     * <p>The engine used to assemble a {@code DistributedNetworkInfo} for this,
     * a snapshot of nine fields of which a consumer wants six. Reading the
     * manager here skips the intermediate entirely; the info type stays because
     * the manager still raises one on every state change.
     */
    @Override
    public MeshState mesh() {
        PeerEndpoint parent = mesh.getParent();
        List<Username> children = mesh.getChildren() == null
                ? List.of()
                : mesh.getChildren().stream()
                        .filter(child -> child != null && isName(child.username()))
                        .map(child -> Username.of(child.username()))
                        .toList();
        return new MeshState(
                mesh.hasParent(),
                user(parent == null ? null : parent.username()),
                children,
                mesh.isBranchRoot(),
                mesh.getBranchLevel(),
                user(mesh.getBranchRoot()));
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
