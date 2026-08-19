// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.Diagnostics;
import dev.slsk.EventStream;
import dev.slsk.diagnostics.DiagnosticLevel;
import dev.slsk.diagnostics.MeshState;
import dev.slsk.diagnostics.Metrics;
import dev.slsk.events.DiagnosticEvent;
import dev.slsk.events.MeshEvent;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.events.DistributedChildEvent;
import dev.slsk.internal.events.DistributedParentEvent;
import dev.slsk.internal.events.EventBus;
import dev.slsk.internal.network.DistributedConnectionManager;
import dev.slsk.internal.network.PeerEndpoint;
import dev.slsk.user.Username;
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
    private final java.util.function.IntSupplier peerConnections;
    private final java.util.function.IntSupplier activeSearches;

    /**
     * The transfer facets, set after construction.
     *
     * <p>They are built after this one and metrics reads both, which is a cycle
     * a constructor cannot express. Set once, by the root type that owns all
     * three.
     */
    private final AtomicReference<dev.slsk.Downloads> downloads = new AtomicReference<>();

    private final AtomicReference<dev.slsk.Uploads> uploads = new AtomicReference<>();
    private final EventBus<DiagnosticEvent> events;
    private final EventBus<MeshEvent> meshEvents;
    private final AtomicReference<MeshState> published = new AtomicReference<>(empty());
    private final AtomicBoolean tracing = new AtomicBoolean();

    DefaultDiagnostics(SoulseekEngine client, EventBus<DiagnosticEvent> events, EventBus<MeshEvent> meshEvents) {
        this.mesh = Objects.requireNonNull(client, "client").getDistributedConnectionManager();
        this.peerConnections = () -> client.getPeerConnectionManager() == null
                        || client.getPeerConnectionManager().getMessageConnections() == null
                ? 0
                : client.getPeerConnectionManager().getMessageConnections().size();
        this.activeSearches = () -> client.getSearches().size();
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
                event.getSource(),
                event.getMessage(),
                Optional.ofNullable(event.getException()),
                event.getTimestamp() == null ? Instant.now() : event.getTimestamp()));
    }

    /**
     * Binds the transfer facets, once the root type has built them.
     *
     * @param downloadFacet the downloads facet
     * @param uploadFacet the uploads facet
     */
    void bind(dev.slsk.Downloads downloadFacet, dev.slsk.Uploads uploadFacet) {
        downloads.set(downloadFacet);
        uploads.set(uploadFacet);
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

    /**
     * Counts what is happening right now.
     *
     * <p>Read on demand rather than accumulated, because everything here is
     * already tracked somewhere that knows it better: the queue knows what is
     * queued, the registries know what is running, and asking them costs less
     * than keeping a second copy in step.
     *
     * <p>Byte totals are the exception — they are cumulative by definition, so
     * they come from counters the transfer path already increments.
     */
    @Override
    public Metrics metrics() {
        // One snapshot of each list, not one per field. Ten separate calls built
        // ten copies of lists that grow with every download the consumer has not
        // forgotten, and a scrape reads several of these — the counts also had
        // no reason to agree with each other, having been taken at ten different
        // moments.
        List<dev.slsk.download.Download> downloadList = downloads.get().all();
        List<dev.slsk.upload.Upload> uploadList = uploads.get().all();

        long downloadedBytes = 0;
        int activeDownloads = 0;
        int queuedDownloads = 0;
        for (dev.slsk.download.Download download : downloadList) {
            downloadedBytes += transferred(download.state());
            if (download.state() instanceof dev.slsk.transfer.TransferState.Queued) {
                queuedDownloads++;
            } else if (running(download.state())) {
                activeDownloads++;
            }
        }

        long uploadedBytes = 0;
        int activeUploads = 0;
        int queuedUploads = 0;
        for (dev.slsk.upload.Upload upload : uploadList) {
            uploadedBytes += transferred(upload.state());
            if (upload.state() instanceof dev.slsk.transfer.TransferState.Queued) {
                queuedUploads++;
            } else if (running(upload.state())) {
                activeUploads++;
            }
        }

        return new Metrics(
                downloadedBytes,
                uploadedBytes,
                activeDownloads,
                activeUploads,
                queuedDownloads,
                queuedUploads,
                peerConnections.getAsInt(),
                activeSearches.getAsInt(),
                0,
                0);
    }

    private static long transferred(dev.slsk.transfer.TransferState state) {
        return state instanceof dev.slsk.transfer.TransferState.Transferring transferring
                ? transferring.progress().transferred()
                : 0;
    }

    private static boolean running(dev.slsk.transfer.TransferState state) {
        return !(state instanceof dev.slsk.transfer.TransferState.Queued)
                && !(state instanceof dev.slsk.transfer.TransferState.Paused)
                && !(state instanceof dev.slsk.transfer.TransferState.Finished);
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
