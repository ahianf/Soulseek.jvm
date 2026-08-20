// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.Subscription;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.internal.ServerLink;
import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.Scheduler;
import dev.slsk.internal.common.TokenFactory;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.concurrent.CancellationController;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.concurrent.CancellationSubscription;
import dev.slsk.internal.concurrent.InterruptedOperationException;
import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.diagnostics.DiagnosticMessage;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.diagnostics.FilteringDiagnosticSink;
import dev.slsk.internal.events.DistributedChildEvent;
import dev.slsk.internal.events.DistributedParentEvent;
import dev.slsk.internal.events.Subscriptions;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.messaging.handlers.DistributedMessageHandler;
import dev.slsk.internal.messaging.messages.AcceptChildrenCommand;
import dev.slsk.internal.messaging.messages.BranchLevelCommand;
import dev.slsk.internal.messaging.messages.BranchRootCommand;
import dev.slsk.internal.messaging.messages.ConnectToPeerRequest;
import dev.slsk.internal.messaging.messages.ConnectToPeerResponse;
import dev.slsk.internal.messaging.messages.DistributedBranchLevel;
import dev.slsk.internal.messaging.messages.DistributedBranchRoot;
import dev.slsk.internal.messaging.messages.EmbeddedMessage;
import dev.slsk.internal.messaging.messages.HaveNoParentsCommand;
import dev.slsk.internal.messaging.messages.PeerInit;
import dev.slsk.internal.messaging.messages.PierceFirewall;
import dev.slsk.internal.network.tcp.ConnectionDisconnectedEvent;
import dev.slsk.internal.network.tcp.ConnectionType;
import dev.slsk.internal.network.tcp.TransportConnection;
import dev.slsk.internal.network.tcp.TransportState;
import dev.slsk.internal.options.SoulseekClientOptions;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Manages distributed-network parent and child connections. */
public final class DistributedNetwork implements DistributedConnectionManager {
    static final int STATUS_AGE_LIMIT = 300_000;
    static final int STATUS_DEBOUNCE_TIME = 5_000;
    static final int WATCHDOG_TIME = 900_000;
    static final double LATENCY_ALPHA = 0.005d;

    /** The live options; a reconfigure replaces them under a running mesh. */
    private final Supplier<SoulseekClientOptions> options;

    private final ServerLink server;
    private final Waiter waiter;
    private final TokenFactory tokens;

    /**
     * What answers a distributed message, supplied late.
     *
     * <p>The handler acts on this mesh and this mesh attaches the handler to
     * every connection it makes, so one of the two has to be told about the
     * other after both exist. The handler is built first, so it is this one.
     */
    private final Supplier<DistributedMessageHandler> distributedMessages;

    private final ConnectionFactory connectionFactory;
    private final DiagnosticSink diagnostic;
    private final Scheduler scheduler;
    private final boolean ownsScheduler;
    private final ScheduledFuture<?> watchdog;
    private final AtomicReference<ScheduledFuture<?>> statusDebounce = new AtomicReference<>();
    private final AtomicBoolean parentConnecting = new AtomicBoolean();
    private final AtomicBoolean statusUpdating = new AtomicBoolean();

    /** Set when a state change arrived while an update was in flight; see updateStatus. */
    private final AtomicBoolean statusDirty = new AtomicBoolean();

    private final AtomicBoolean closed = new AtomicBoolean();
    private final ConcurrentHashMap<String, ConnectionCell> childConnections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, InetSocketAddress> children = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CancellationController> pendingInboundIndirectConnections =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, String> pendingSolicitations = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MessageConnection, Subscription> childDisconnectSubscriptions =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MessageConnection, Subscription> parentCandidateDisconnectSubscriptions =
            new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<Consumer<? super DistributedChildEvent>> childAddedListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super DistributedChildEvent>> childDisconnectedListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super Void>> demotedListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super DistributedParentEvent>> parentAdoptedListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super DistributedParentEvent>> parentDisconnectedListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super Void>> promotedListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super DistributedNetworkInfo>> stateChangedListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super DiagnosticMessage>> diagnosticListeners =
            new CopyOnWriteArrayList<>();
    private final Consumer<ConnectionDisconnectedEvent> parentCandidateDisconnectedListener =
            this::parentCandidateDisconnected;
    private final Consumer<ConnectionDisconnectedEvent> parentDisconnectedListener = this::parentDisconnected;
    private final Consumer<ConnectionDisconnectedEvent> childDisconnectedListener = this::childDisconnected;
    private final Consumer<MessageEvent> parentInitializationListener = this::handleParentCandidateMessage;

    private volatile Double averageBroadcastLatency;
    private volatile boolean branchRootNode;
    private volatile String lastStatus;
    private volatile Instant lastStatusTimestamp;
    private volatile int parentBranchLevel;
    private volatile String parentBranchRoot = "";
    private volatile List<PeerEndpoint> parentCandidates = List.of();
    private volatile MessageConnection parentConnection;
    private volatile Subscription parentDisconnectSubscription;

    /** Creates a distributed network with default collaborators. */
    public DistributedNetwork(
            Supplier<SoulseekClientOptions> options,
            ServerLink server,
            Waiter waiter,
            TokenFactory tokens,
            Supplier<DistributedMessageHandler> distributedMessages,
            ConnectionFactory connectionFactory) {
        this(options, server, waiter, tokens, distributedMessages, connectionFactory, null, null);
    }

    /** Creates a distributed network. */
    public DistributedNetwork(
            Supplier<SoulseekClientOptions> options,
            ServerLink server,
            Waiter waiter,
            TokenFactory tokens,
            Supplier<DistributedMessageHandler> distributedMessages,
            ConnectionFactory connectionFactory,
            DiagnosticSink diagnosticFactory) {
        this(options, server, waiter, tokens, distributedMessages, connectionFactory, diagnosticFactory, null);
    }

    /**
     * Creates a distributed network sharing a caller-owned scheduler.
     *
     * @param options the live client options
     * @param server the server link, for our username, the client state and the
     *     solicitations this sends
     * @param waiter the response correlator
     * @param tokens the token allocator
     * @param distributedMessages what answers a distributed message
     * @param connectionFactory the connection factory
     * @param diagnosticFactory the diagnostic sink
     * @param scheduler the shared scheduler, or {@code null} to own one
     */
    public DistributedNetwork(
            Supplier<SoulseekClientOptions> options,
            ServerLink server,
            Waiter waiter,
            TokenFactory tokens,
            Supplier<DistributedMessageHandler> distributedMessages,
            ConnectionFactory connectionFactory,
            DiagnosticSink diagnosticFactory,
            Scheduler scheduler) {
        this.options = Objects.requireNonNull(options, "options");
        this.server = Objects.requireNonNull(server, "server");
        this.waiter = Objects.requireNonNull(waiter, "waiter");
        this.tokens = Objects.requireNonNull(tokens, "tokens");
        this.distributedMessages = Objects.requireNonNull(distributedMessages, "distributedMessages");
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory");
        diagnostic = diagnosticFactory == null
                ? new FilteringDiagnosticSink(options.get().minimumDiagnosticLevel(), this::publishDiagnostic)
                : DiagnosticSink.forSource(diagnosticFactory, DistributedNetwork.class);
        this.ownsScheduler = scheduler == null;
        this.scheduler = scheduler == null ? new Scheduler("soulseek-distributed-status") : scheduler;
        watchdog = this.scheduler.scheduleAtFixedRate(
                this::watchdogElapsed, WATCHDOG_TIME, WATCHDOG_TIME, TimeUnit.MILLISECONDS);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Subscription subscribe(Kind kind, Consumer<? super T> listener) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(listener, "listener");
        return switch (kind) {
            case CHILD_ADDED ->
                Subscriptions.add(childAddedListeners, (Consumer<? super DistributedChildEvent>) listener);
            case CHILD_DISCONNECTED ->
                Subscriptions.add(childDisconnectedListeners, (Consumer<? super DistributedChildEvent>) listener);
            case DEMOTED_FROM_BRANCH_ROOT -> Subscriptions.add(demotedListeners, (Consumer<? super Void>) listener);
            case PARENT_ADOPTED ->
                Subscriptions.add(parentAdoptedListeners, (Consumer<? super DistributedParentEvent>) listener);
            case PARENT_DISCONNECTED ->
                Subscriptions.add(parentDisconnectedListeners, (Consumer<? super DistributedParentEvent>) listener);
            case PROMOTED_TO_BRANCH_ROOT -> Subscriptions.add(promotedListeners, (Consumer<? super Void>) listener);
            case STATE_CHANGED ->
                Subscriptions.add(stateChangedListeners, (Consumer<? super DistributedNetworkInfo>) listener);
        };
    }

    @Override
    public Subscription subscribe(Consumer<? super DiagnosticMessage> listener) {
        return Subscriptions.add(diagnosticListeners, listener);
    }

    @Override
    public Double getAverageBroadcastLatency() {
        return averageBroadcastLatency;
    }

    @Override
    public int getBranchLevel() {
        return hasParent() ? parentBranchLevel + 1 : 0;
    }

    @Override
    public String getBranchRoot() {
        String value = hasParent() ? parentBranchRoot : server.username();
        return value == null ? "" : value;
    }

    @Override
    public boolean canAcceptChildren() {
        return isEnabled()
                && isAcceptingChildren()
                && (hasParent() || isBranchRoot())
                && children.size() < getChildLimit();
    }

    @Override
    public int getChildLimit() {
        return options.get().distributedChildLimit();
    }

    @Override
    public List<PeerEndpoint> getChildren() {
        return children.entrySet().stream()
                .map(entry -> new PeerEndpoint(entry.getKey(), entry.getValue()))
                .toList();
    }

    @Override
    public boolean hasParent() {
        MessageConnection current = parentConnection;
        return current != null && current.getState() == TransportState.CONNECTED;
    }

    @Override
    public boolean isBranchRoot() {
        return branchRootNode;
    }

    @Override
    public PeerEndpoint getParent() {
        MessageConnection current = parentConnection;
        return current == null
                ? new PeerEndpoint("", null)
                : new PeerEndpoint(current.getUsername(), current.getIpEndpoint());
    }

    @Override
    public Map<Integer, String> getPendingSolicitations() {
        return Map.copyOf(pendingSolicitations);
    }

    @Override
    public void addOrUpdateChildConnection(String username, TransportConnection incomingConnection) {
        Objects.requireNonNull(incomingConnection, "incomingConnection");
        if (!canAcceptChildren()) {
            diagnostic.debug(rejectionMessage(username, incomingConnection.getIpEndpoint()));
            incomingConnection.close();
            updateStatus();
            return;
        }

        // A child that connects to us wins over whatever we had for it, so this
        // claims the entry outright. It used to be claimed inside a compute,
        // which held the map's bin lock for as long as the establishment took.
        ConnectionCell cell = new ConnectionCell();
        ConnectionCell superseded = childConnections.put(username, cell);

        try {
            cell.settle(establishDirectChild(username, incomingConnection, superseded));
        } catch (Throwable cause) {
            cell.fail(cause);
            String message = "Failed to establish an inbound direct child connection "
                    + "to " + username + " ("
                    + incomingConnection.getIpEndpoint() + "): "
                    + Failures.message(cause);
            diagnostic.debug(
                    message + " (type: " + incomingConnection.getType() + ", id: " + incomingConnection.getId() + ")");
            diagnostic.debug("Purging child connection cache of failed connection to "
                    + username + " ("
                    + incomingConnection.getIpEndpoint() + ")");
            childConnections.remove(username, cell);
            throw new ConnectionException(message, cause);
        }
    }

    /**
     * Attempts every candidate at once and adopts the one with the lowest branch
     * level.
     *
     * <p>One thread per candidate, and every one of them is joined before any is
     * sorted. This replaced a {@code CompletableFuture.allOf} over the same
     * threads, which read the results back out of the tasks it had just waited
     * on: a candidate whose task had settled but whose completion had not yet
     * propagated was filtered out of the successful list, so on a loaded machine
     * the mesh adopted the wrong parent and left the loser connected.
     *
     * <p>The race inside {@link #getParentCandidateConnection} is a different
     * thing and stays: direct against indirect for one candidate, first to
     * answer wins. This wants all of them and then the best.
     */
    @Override
    public void addParentConnection(Iterable<PeerEndpoint> candidates) {
        if (!isEnabled()) {
            diagnostic.debug("Parent connection solicitation ignored; distributed " + "network is not enabled.");
            return;
        }
        SoulseekClientState state = server.state();
        if (state == SoulseekClientState.DISCONNECTED || state == SoulseekClientState.DISCONNECTING) {
            return;
        }

        List<PeerEndpoint> snapshot = new ArrayList<>();
        candidates.forEach(snapshot::add);
        parentCandidates = List.copyOf(snapshot);
        if (hasParent() || snapshot.isEmpty()) {
            diagnostic.debug(
                    hasParent()
                            ? "Parent connection solicitation ignored; already " + "connected to parent "
                                    + getParent().username()
                            : "Parent candidate cache is empty; requesting a new list "
                                    + "of candidates from the server");
            updateStatus();
            return;
        }
        if (!parentConnecting.compareAndSet(false, true)) {
            diagnostic.debug("Parent connection solicitation ignored; already in the "
                    + "process of establishing a connection.");
            return;
        }

        diagnostic.info("Attempting to establish a new parent connection from " + snapshot.size() + " candidates");
        diagnostic.debug("Parent candidates: "
                + String.join(
                        ", ", snapshot.stream().map(PeerEndpoint::username).toList()));
        CancellationController cancellation = new CancellationController();
        try {
            adoptBestCandidate(attemptCandidates(snapshot, cancellation.getSignal()));
        } finally {
            cancellation.close();
            parentConnecting.set(false);
            updateStatus();
        }
    }

    /**
     * Attempts every candidate on a thread of its own and returns those that
     * answered, lowest branch level first.
     */
    private List<ParentCandidate> attemptCandidates(
            List<PeerEndpoint> snapshot, CancellationSignal cancellationSignal) {
        ExecutorService executor = scheduler.executor();
        List<Future<ParentCandidate>> attempts = new ArrayList<>(snapshot.size());
        for (PeerEndpoint candidate : snapshot) {
            attempts.add(executor.submit(() ->
                    getParentCandidateConnection(candidate.username(), candidate.ipEndpoint(), cancellationSignal)));
        }

        List<ParentCandidate> successful = new ArrayList<>(attempts.size());
        for (Future<ParentCandidate> attempt : attempts) {
            ParentCandidate candidate;
            try {
                candidate = attempt.get();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException | CancellationException failure) {
                // Every attempt reports its own failure on the way out; one
                // candidate refusing us says nothing about the rest.
                continue;
            }
            if (candidate != null && candidate.connection().getState() == TransportState.CONNECTED) {
                successful.add(candidate);
            }
        }
        successful.sort(Comparator.comparingInt(ParentCandidate::branchLevel));
        return successful;
    }

    /** Adopts the lowest-branch-level candidate and closes of the rest. */
    private void adoptBestCandidate(List<ParentCandidate> successful) {
        if (successful.isEmpty()) {
            diagnostic.warning("Failed to connect to any of the available parent " + "candidates");
            return;
        }

        diagnostic.debug("Successfully established " + successful.size() + " connections.");
        ParentCandidate selected = successful.getFirst();
        List<ParentCandidate> rejected = successful.subList(1, successful.size());
        parentConnection = selected.connection();
        parentBranchLevel = selected.branchLevel();
        parentBranchRoot = selected.branchRoot();
        diagnostic.debug("Selected " + parentConnection.getUsername()
                + " as the best connection; branch root: "
                + parentBranchRoot + ", branch level: "
                + parentBranchLevel);
        parentDisconnectSubscription =
                parentConnection.subscribe(TransportConnection.Kind.DISCONNECTED, parentDisconnectedListener);
        Subscription candidateSubscription = parentCandidateDisconnectSubscriptions.remove(parentConnection);
        if (candidateSubscription != null) {
            candidateSubscription.close();
        }
        DistributedMessageHandler handler = distributedMessages.get();
        parentConnection.<MessageEvent>subscribe(MessageConnection.MessageKind.READ, handler::handleMessageRead);
        parentConnection.<MessageEvent>subscribe(MessageConnection.MessageKind.WRITTEN, handler::handleMessageWritten);
        diagnostic.debug("Parent connection to " + parentConnection.getUsername()
                + " (" + parentConnection.getIpEndpoint()
                + ") established. (type: " + parentConnection.getType()
                + ", id: " + parentConnection.getId() + ")");
        diagnostic.info("Adopted parent connection to "
                + parentConnection.getUsername() + " ("
                + parentConnection.getIpEndpoint() + ")");
        demoteFromBranchRoot();
        DistributedParentEvent eventData = new DistributedParentEvent(
                parentConnection.getUsername(), parentConnection.getIpEndpoint(), parentBranchLevel, parentBranchRoot);
        parentAdoptedListeners.forEach(listener -> listener.accept(eventData));
        publishStateChanged();

        parentCandidates = rejected.stream()
                .map(candidate -> new PeerEndpoint(
                        candidate.connection().getUsername(),
                        candidate.connection().getIpEndpoint()))
                .toList();
        diagnostic.debug("Connected parent candidates not selected: "
                + (parentCandidates.isEmpty()
                        ? "<none>"
                        : String.join(
                                ", ",
                                parentCandidates.stream()
                                        .map(PeerEndpoint::username)
                                        .toList())));
        rejected.forEach(candidate -> {
            diagnostic.debug("Disconnecting parent candidate connection to "
                    + candidate.connection().getUsername() + " ("
                    + candidate.connection().getIpEndpoint() + ")");
            candidate.connection().disconnect("Not selected.");
            candidate.connection().close();
        });
        updateStatus();
        broadcastMessage(getBranchInformation(), CancellationSignal.none());
    }

    /**
     * Writes a message to every child, blocking until all of them have settled.
     *
     * <p>An established child takes the two-phase path: the frame is enqueued
     * here and written by that connection's own writer, so delivery to every
     * child proceeds in parallel without a thread per child per message. The
     * JFR baseline put the old per-child virtual threads at 4% of the client's
     * total allocation pressure — this runs for every search request forwarded
     * down the branch. Only a child still completing its handshake gets a
     * thread of its own, to wait for its cell without holding up the rest.
     *
     * <p>A failing child is disconnected and does not affect the others, which
     * is what the per-child {@code exceptionally} used to guarantee.
     */
    @Override
    public void broadcastMessage(byte[] bytes, CancellationSignal cancellationSignal) {
        long started = System.nanoTime();
        CancellationSignal effectiveToken = token(cancellationSignal);

        List<PendingChildWrite> pending = new ArrayList<>(childConnections.size());
        List<Future<?>> establishing = null;
        for (ConnectionCell cell : childConnections.values()) {
            MessageConnection connection = cell.peek();
            if (connection == null) {
                if (establishing == null) {
                    establishing = new ArrayList<>();
                }
                establishing.add(scheduler.executor().submit(() -> writeToChild(cell, bytes, effectiveToken)));
                continue;
            }
            if (connection.getState() != TransportState.CONNECTED) {
                continue;
            }
            try {
                pending.add(new PendingChildWrite(connection, connection.beginWrite(bytes, effectiveToken)));
            } catch (Exception failure) {
                connection.disconnect("Broadcast failure: " + Failures.message(failure));
            }
        }

        for (PendingChildWrite write : pending) {
            try {
                write.pending().await();
            } catch (InterruptedOperationException interrupted) {
                // The per-child future wait used to return on the broadcast
                // caller's interrupt; keep that.
                Thread.currentThread().interrupt();
                return;
            } catch (Exception failure) {
                write.connection().disconnect("Broadcast failure: " + Failures.message(failure));
            }
        }

        if (establishing != null) {
            for (Future<?> write : establishing) {
                try {
                    write.get();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (ExecutionException failure) {
                    // Already handled per child in writeToChild; one child must
                    // not abort the broadcast to the rest.
                }
            }
        }

        double elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        Double current = averageBroadcastLatency;
        averageBroadcastLatency = current == null ? elapsed : ((elapsed - current) * LATENCY_ALPHA) + current;
    }

    private record PendingChildWrite(MessageConnection connection, TransportConnection.PendingWrite pending) {}

    private void writeToChild(ConnectionCell pending, byte[] bytes, CancellationSignal cancellationSignal) {
        // The child may still be completing its handshake; one that failed it
        // answers null rather than throwing.
        MessageConnection connection;
        try {
            connection = pending.awaitQuietly();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return;
        }
        if (connection == null || connection.getState() != TransportState.CONNECTED) {
            return;
        }
        try {
            // Written on this thread rather than dispatched and joined. This
            // method already runs on a virtual thread of its own, one per
            // child, so dispatching the write bought a second thread per
            // child per message whose only purpose was to be waited on.
            connection.write(bytes, cancellationSignal);
        } catch (Exception failure) {
            connection.disconnect("Broadcast failure: " + Failures.message(failure));
        }
    }

    @Override
    public void demoteFromBranchRoot() {
        if (branchRootNode) {
            branchRootNode = false;
            diagnostic.info("Demoted from distributed branch root.");
            demotedListeners.forEach(listener -> listener.accept(null));
            publishStateChanged();
        }
    }

    @Override
    public void getOrAddChildConnection(ConnectToPeerResponse response) {
        if (!canAcceptChildren()) {
            diagnostic.debug(rejectionMessage(response.getUsername(), response.getIpEndpoint()));
            updateStatus();
            return;
        }

        String username = response.getUsername();
        ConnectionCell claim = new ConnectionCell();
        ConnectionCell cached = childConnections.putIfAbsent(username, claim);
        ConnectionCell entry = cached == null ? claim : cached;

        try {
            if (cached != null) {
                entry.await();
                diagnostic.debug("Child connection from " + username
                        + " (" + response.getIpEndpoint()
                        + ") for token " + response.getToken()
                        + " ignored; connection already exists.");
                return;
            }
            entry.settle(establishIndirectChild(response));
        } catch (Throwable cause) {
            entry.fail(cause);
            String message = "Failed to establish an inbound indirect child connection "
                    + "to " + username + " ("
                    + response.getIpEndpoint() + "): " + Failures.message(cause);
            diagnostic.debug(message);
            if (!(cause instanceof CancellationException)) {
                diagnostic.debug("Purging child connection cache of failed connection to "
                        + username + " ("
                        + response.getIpEndpoint() + ").");
                // Only ever the entry this attempt was waiting on. A direct
                // child that superseded it has already replaced the entry, and
                // purging that one is what the "erroneously purged" warning
                // used to detect after the fact and undo.
                childConnections.remove(username, entry);
            }
            throw new ConnectionException(message, cause);
        }
    }

    @Override
    public void promoteToBranchRoot() {
        if (!branchRootNode && !hasParent()) {
            branchRootNode = true;
            diagnostic.info("Promoted to distributed branch root.");
            promotedListeners.forEach(listener -> listener.accept(null));
            publishStateChanged();
        }
    }

    @Override
    public void removeAndCloseAll() {
        pendingSolicitations.clear();
        pendingInboundIndirectConnections.clear();
        MessageConnection parent = parentConnection;
        if (parent != null) {
            parent.close();
        }
        parentConnection = null;
        childConnections.forEach((username, cell) -> {
            if (childConnections.remove(username, cell)) {
                cell.closeWhenSettled();
            }
        });
        children.clear();
    }

    @Override
    public void resetStatus() {
        lastStatus = null;
        lastStatusTimestamp = null;
        demoteFromBranchRoot();
    }

    @Override
    public void setParentBranchLevel(int branchLevel) {
        parentBranchLevel = branchLevel;
        updateStatusEventually();
    }

    @Override
    public void setParentBranchRoot(String branchRoot) {
        parentBranchRoot = branchRoot;
        updateStatusEventually();
    }

    @Override
    public void updateStatus(CancellationSignal cancellationSignal) {
        SoulseekClientState state = server.state();
        if (!state.isLoggedIn()) {
            return;
        }
        while (true) {
            if (!statusUpdating.compareAndSet(false, true)) {
                // The in-flight updater recomputes before returning rather
                // than dropping this state change.
                statusDirty.set(true);
                return;
            }

            try {
                do {
                    statusDirty.set(false);
                    sendStatus(cancellationSignal);
                } while (statusDirty.get());
            } finally {
                statusUpdating.set(false);
            }
            if (!statusDirty.get()) {
                return;
            }
            // A caller landed in the release window. Iterate and either take
            // ownership again or mark the updater that beat us as dirty.
        }
    }

    /** Computes and sends the status once; only {@link #updateStatus} calls this. */
    private void sendStatus(CancellationSignal cancellationSignal) {
        int branchLevel = getBranchLevel();
        String branchRoot = getBranchRoot();
        boolean accept = canAcceptChildren();
        boolean haveNoParents = isEnabled() && !hasParent();
        String status = "Requesting parent: " + haveNoParents
                + ", Branch level: " + branchLevel
                + ", Branch root: " + branchRoot
                + ", Number of children: " + children.size() + "/"
                + getChildLimit()
                + ", Accepting children: " + accept;
        if (lastStatus != null && lastStatus.equalsIgnoreCase(status)) {
            diagnostic.debug("Update skipped; status has not changed: " + status);
            return;
        }

        diagnostic.debug("Status changed; " + status);
        byte[] payload = concatenate(
                new BranchLevelCommand(branchLevel).toByteArray(),
                new BranchRootCommand(branchRoot).toByteArray(),
                new AcceptChildrenCommand(accept).toByteArray(),
                new HaveNoParentsCommand(haveNoParents).toByteArray());
        try {
            server.writeBytes(payload, token(cancellationSignal));
            publishStateChanged();
            diagnostic.info("Updated distributed status; " + status);
            lastStatus = status;
            lastStatusTimestamp = Instant.now();
        } catch (Throwable failure) {
            // A status update is nobody's to fail: every caller here is a
            // state change reporting itself, not a request with a waiter.
            Throwable cause = failure;
            String message = "Failed to update distributed status: " + Failures.message(cause);
            if (!server.state().equals(SoulseekClientState.DISCONNECTED)) {
                diagnostic.warning(message, cause);
            } else {
                diagnostic.debug(message, cause);
            }
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            watchdog.cancel(false);
            ScheduledFuture<?> debounce = statusDebounce.getAndSet(null);
            if (debounce != null) {
                debounce.cancel(false);
            }
            if (ownsScheduler) {
                scheduler.close();
            }
            removeAndCloseAll();
        }
    }

    byte[] getBranchInformation() {
        return concatenate(
                new DistributedBranchLevel(getBranchLevel()).toByteArray(),
                new DistributedBranchRoot(getBranchRoot()).toByteArray());
    }

    void watchdogElapsed() {
        SoulseekClientState state = server.state();
        if (isEnabled() && !hasParent() && !isBranchRoot() && state.isLoggedIn()) {
            diagnostic.warning("No distributed parent connected.  Requesting a list of " + "candidates.");
            updateStatus();
        }
    }

    void handleParentCandidateMessage(MessageEvent eventData) {
        MessageConnection connection = eventData.connection();
        try {
            byte[] message = eventData.message();
            MessageCode.Distributed code = new MessageReader<>(message, MessageCode.Distributed.class).readCode();
            switch (code) {
                case EMBEDDED_MESSAGE -> {
                    EmbeddedMessage embedded = EmbeddedMessage.fromByteArray(message);
                    if (embedded.getDistributedCode() == MessageCode.Distributed.SEARCH_REQUEST) {
                        waiter.complete(new WaitKey.SearchRequest(connection.getId()));
                    }
                }
                case SEARCH_REQUEST -> waiter.complete(new WaitKey.SearchRequest(connection.getId()));
                case BRANCH_LEVEL ->
                    waiter.complete(
                            new WaitKey.BranchLevel(connection.getId()),
                            DistributedBranchLevel.fromByteArray(message).getLevel());
                case BRANCH_ROOT ->
                    waiter.complete(
                            new WaitKey.BranchRoot(connection.getId()),
                            DistributedBranchRoot.fromByteArray(message).getUsername());
                default -> {
                    // Source ignores all other distributed messages here.
                }
            }
        } catch (Throwable failure) {
            diagnostic.debug("Failed to handle message from parent candidate: " + Failures.message(failure), failure);
            connection.disconnect(Failures.message(failure));
            connection.close();
        }
    }

    private MessageConnection establishDirectChild(
            String username, TransportConnection incomingConnection, ConnectionCell cached)
            throws InterruptedException, TimeoutException {
        diagnostic.debug("Inbound child connection to " + username + " ("
                + incomingConnection.getIpEndpoint()
                + ") accepted. (type: " + incomingConnection.getType()
                + ", id: " + incomingConnection.getId());
        MessageConnection connection = connectionFactory.getDistributedConnection(
                username,
                incomingConnection.getIpEndpoint(),
                options.get().distributedConnectionOptions(),
                incomingConnection.handoffConnector());
        diagnostic.debug("Inbound child connection to " + username + " ("
                + connection.getIpEndpoint() + ") handed off. (old: "
                + incomingConnection.getId() + ", new: "
                + connection.getId() + ")");
        incomingConnection.close();
        connection.setType(ConnectionType.INBOUND_DIRECT);
        attachChildMessageListeners(connection);
        connection.subscribe(
                TransportConnection.Kind.DISCONNECTED,
                (ConnectionDisconnectedEvent event) -> event.connection().close());
        boolean superseded = false;

        if (cached != null) {
            CancellationController pending = pendingInboundIndirectConnections.get(username);
            if (pending != null) {
                diagnostic.debug("Cancelling pending indirect child connection to " + username);
                pending.cancel();
            }
            // An attempt still in flight owns the connection it is about to
            // produce, so it has to finish before that connection can be let go
            // of. Cancelling the pending indirect above is what makes it finish
            // promptly; every other attempt is bounded by its connect timeout.
            MessageConnection old = cached.awaitQuietly();
            if (old != null) {
                Subscription oldSubscription = childDisconnectSubscriptions.remove(old);
                if (oldSubscription != null) {
                    oldSubscription.close();
                }
                diagnostic.debug("Superseding existing child connection to "
                        + username + " (" + old.getIpEndpoint()
                        + ") (old: " + incomingConnection.getId()
                        + ", new: " + connection.getId());
                old.disconnect("Superseded.");
                old.close();
                superseded = true;
            }
        }

        try {
            connection.startReadingContinuously();
            connection.write(getBranchInformation());
        } catch (Throwable failure) {
            connection.close();
            throw Failures.rethrow(failure);
        }
        subscribeToChildDisconnect(connection);
        children.put(username, connection.getIpEndpoint());
        diagnostic.debug("Child connection to " + connection.getUsername()
                + " (" + connection.getIpEndpoint()
                + ") established. (type: "
                + connection.getType() + ", id: "
                + connection.getId() + ")");
        diagnostic.info((superseded ? "Updated" : "Added")
                + " child connection to "
                + connection.getUsername() + " ("
                + connection.getIpEndpoint() + ")");
        if (!superseded) {
            publishChildAdded(connection);
            publishStateChanged();
        }
        updateStatusEventually();
        return connection;
    }

    private MessageConnection establishIndirectChild(ConnectToPeerResponse response)
            throws InterruptedException, TimeoutException {
        diagnostic.debug("Attempting inbound indirect child connection to "
                + response.getUsername() + " (" + response.getIpEndpoint()
                + ") for token " + response.getToken());
        MessageConnection connection = connectionFactory.getDistributedConnection(
                response.getUsername(), response.getIpEndpoint(), options.get().distributedConnectionOptions());
        connection.setType(ConnectionType.INBOUND_INDIRECT);
        attachChildMessageListeners(connection);
        connection.subscribe(
                TransportConnection.Kind.DISCONNECTED,
                (ConnectionDisconnectedEvent event) -> event.connection().close());
        CancellationController cancellation = new CancellationController();
        pendingInboundIndirectConnections.put(response.getUsername(), cancellation);

        try {
            connection.connect(cancellation.getSignal());
            connection.write(new PierceFirewall(response.getToken()).toByteArray(), cancellation.getSignal());
            connection.write(getBranchInformation(), cancellation.getSignal());
        } catch (Throwable failure) {
            connection.close();
            throw Failures.rethrow(failure);
        } finally {
            pendingInboundIndirectConnections.remove(response.getUsername(), cancellation);
            cancellation.close();
        }
        subscribeToChildDisconnect(connection);
        children.put(response.getUsername(), connection.getIpEndpoint());
        diagnostic.debug("Child connection to " + connection.getUsername() + " ("
                + connection.getIpEndpoint()
                + ") established. (type: " + connection.getType()
                + ", id: " + connection.getId() + ")");
        diagnostic.info(
                "Added child connection to " + connection.getUsername() + " (" + connection.getIpEndpoint() + ")");
        publishChildAdded(connection);
        publishStateChanged();
        updateStatusEventually();
        return connection;
    }

    private ParentCandidate getParentCandidateConnection(
            String username, InetSocketAddress ipEndpoint, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        LinkedCancellation directCancellation = new LinkedCancellation(cancellationSignal);
        LinkedCancellation indirectCancellation = new LinkedCancellation(cancellationSignal);
        diagnostic.debug("Attempting simultaneous direct and indirect parent candidate " + "connections to " + username
                + " (" + ipEndpoint + ")");

        FirstSuccess.Winner<MessageConnection> winner;
        try {
            winner = FirstSuccess.race(
                    scheduler.executor(),
                    () -> getParentCandidateConnectionDirect(username, ipEndpoint, directCancellation.token()),
                    () -> getParentCandidateConnectionIndirect(username, indirectCancellation.token()));
        } catch (Throwable failure) {
            directCancellation.close();
            indirectCancellation.close();
            if (failure instanceof ConnectionException connectionFailure) {
                throw connectionFailure;
            }
            String message = "Failed to establish a direct or indirect parent "
                    + "candidate connection to " + username + " ("
                    + ipEndpoint + ")";
            diagnostic.debug(message);
            throw new ConnectionException(message);
        }

        boolean directWon = winner.first();
        MessageConnection connection = winner.value();
        diagnostic.debug((directWon ? "Direct" : "Indirect")
                + " parent candidate connection to " + username + " ("
                + ipEndpoint
                + ") established first, attempting to cancel "
                + (directWon ? "indirect" : "direct") + " connection.");
        (directWon ? indirectCancellation : directCancellation).cancel();

        // Registered before the negotiation that provokes them: the candidate
        // sends its branch information the moment it hears from us.
        Wait<BranchInformation> initialization = registerParentCandidateInitialization(connection, cancellationSignal);
        try {
            if (directWon) {
                connection.write(
                        new PeerInit(server.username(), Constants.ConnectionType.DISTRIBUTED, tokens.nextToken())
                                .toByteArray(),
                        token(cancellationSignal));
            } else {
                connection.startReadingContinuously();
            }
            diagnostic.debug((directWon ? "Direct" : "Indirect")
                    + " parent candidate connection to " + username + " ("
                    + ipEndpoint + ") initialized.  Waiting for branch "
                    + "information and first search request. (id: "
                    + connection.getId() + ")");
            BranchInformation branch = initialization.await();
            diagnostic.debug("Parent candidate connection to " + username + " ("
                    + ipEndpoint + ") established. (type: "
                    + connection.getType() + ", id: "
                    + connection.getId() + ")");
            return new ParentCandidate(connection, branch.level(), branch.root());
        } catch (Throwable cause) {
            String message = "Failed to negotiate parent candidate "
                    + "connection to " + username + " ("
                    + ipEndpoint + "): " + Failures.message(cause);
            diagnostic.debug(message + " (type: " + connection.getType() + ", id: " + connection.getId() + ")");
            connection.close();
            throw new ConnectionException(message, cause);
        } finally {
            directCancellation.close();
            indirectCancellation.close();
        }
    }

    private MessageConnection getParentCandidateConnectionDirect(
            String username, InetSocketAddress ipEndpoint, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        diagnostic.debug("Attempting direct parent candidate connection to " + username + " (" + ipEndpoint + ")");
        MessageConnection connection = connectionFactory.getDistributedConnection(
                username, ipEndpoint, options.get().distributedConnectionOptions());
        connection.setType(ConnectionType.OUTBOUND_DIRECT);
        subscribeToParentCandidateDisconnect(connection);
        try {
            connection.connect(cancellationSignal);
        } catch (Throwable failure) {
            diagnostic.debug("Failed to establish a direct parent candidate "
                    + "connection to " + username + " ("
                    + ipEndpoint + "): "
                    + Failures.message(failure));
            connection.close();
            throw Failures.rethrow(failure);
        }
        diagnostic.debug("Direct parent candidate connection to " + username
                + " (" + connection.getIpEndpoint()
                + ") established. (type: " + connection.getType()
                + ", id: " + connection.getId() + ")");
        return connection;
    }

    private MessageConnection getParentCandidateConnectionIndirect(
            String username, CancellationSignal cancellationSignal) throws InterruptedException, TimeoutException {
        int solicitationToken = tokens.nextToken();
        diagnostic.debug(
                "Soliciting indirect parent candidate connection to " + username + " with token " + solicitationToken);
        pendingSolicitations.putIfAbsent(solicitationToken, username);
        try {
            Wait<TransportConnection> wait = waiter.register(
                    new WaitKey.SolicitedDistributed(username, solicitationToken),
                    TransportConnection.class,
                    options.get().distributedConnectionOptions().connectTimeout(),
                    cancellationSignal);
            server.write(
                    new ConnectToPeerRequest(solicitationToken, username, Constants.ConnectionType.DISTRIBUTED),
                    cancellationSignal);
            TransportConnection accepted = wait.await();
            try {
                MessageConnection connection = connectionFactory.getDistributedConnection(
                        username,
                        accepted.getIpEndpoint(),
                        options.get().distributedConnectionOptions(),
                        accepted.handoffConnector());
                diagnostic.debug("Indirect parent candidate connection to " + username
                        + " (" + accepted.getIpEndpoint()
                        + ") handed off. (old: " + accepted.getId()
                        + ", new: " + connection.getId() + ")");
                connection.setType(ConnectionType.OUTBOUND_INDIRECT);
                subscribeToParentCandidateDisconnect(connection);
                diagnostic.debug("Indirect parent candidate connection to " + username
                        + " (" + connection.getIpEndpoint()
                        + ") established. (type: "
                        + connection.getType() + ", id: "
                        + connection.getId() + ")");
                return connection;
            } finally {
                accepted.close();
            }
        } catch (Throwable failure) {
            diagnostic.debug("Failed to establish an indirect parent candidate "
                    + "connection to " + username + " with token "
                    + solicitationToken + ": "
                    + Failures.message(failure));
            throw Failures.rethrow(failure);
        } finally {
            pendingSolicitations.remove(solicitationToken, username);
        }
    }

    private Wait<BranchInformation> registerParentCandidateInitialization(
            MessageConnection connection, CancellationSignal cancellationSignal) {
        Subscription initializationSubscription =
                connection.subscribe(MessageConnection.MessageKind.READ, parentInitializationListener);
        // All three are registered before any is awaited: the candidate sends
        // them back to back and a wait registered after the first would miss
        // the ones behind it.
        Wait<Integer> branchLevel = waiter.register(
                new WaitKey.BranchLevel(connection.getId()),
                Integer.class,
                waiter.getDefaultTimeout(),
                token(cancellationSignal));
        Wait<String> branchRoot = waiter.register(
                new WaitKey.BranchRoot(connection.getId()),
                String.class,
                waiter.getDefaultTimeout(),
                token(cancellationSignal));
        Wait<Void> search = waiter.register(
                new WaitKey.SearchRequest(connection.getId()), waiter.getDefaultTimeout(), token(cancellationSignal));
        // The three waits are live from here; awaiting them is the caller's,
        // after it has written whatever provokes the candidate into answering.
        return () -> {
            try {
                int level = branchLevel.await();
                search.await();
                if (level > 0) {
                    return new BranchInformation(level, branchRoot.await());
                }
                diagnostic.debug("Received branch level 0 from parent candidate "
                        + connection.getUsername()
                        + "; this user is a branch root.");
                return new BranchInformation(level, connection.getUsername());
            } catch (Throwable failure) {
                connection.disconnect("One or more required messages was not received.");
                throw new ConnectionException("Failed to retrieve branch info from parent "
                        + "candidate connection to "
                        + connection.getUsername() + " ("
                        + connection.getIpEndpoint()
                        + "); one or more required messages was not "
                        + "received. (id: " + connection.getId() + ")");
            } finally {
                initializationSubscription.close();
            }
        };
    }

    private void attachChildMessageListeners(MessageConnection connection) {
        DistributedMessageHandler handler = distributedMessages.get();
        connection.<MessageEvent>subscribe(MessageConnection.MessageKind.READ, handler::handleChildMessageRead);
        connection.<MessageEvent>subscribe(MessageConnection.MessageKind.WRITTEN, handler::handleChildMessageWritten);
    }

    private void childDisconnected(ConnectionDisconnectedEvent eventData) {
        MessageConnection connection = (MessageConnection) eventData.connection();
        Subscription subscription = childDisconnectSubscriptions.remove(connection);
        if (subscription != null) {
            subscription.close();
        }
        childConnections.remove(connection.getUsername());
        children.remove(connection.getUsername());
        diagnostic.debug("Child connection to " + connection.getUsername() + " ("
                + connection.getIpEndpoint() + ") disconnected: "
                + eventData.message() + " (type: "
                + connection.getType() + ", id: "
                + connection.getId() + ")");
        diagnostic.info("Child connection to " + connection.getUsername() + " ("
                + connection.getIpEndpoint() + ") disconnected"
                + (eventData.message() == null ? "." : ": " + eventData.message()));
        DistributedChildEvent childEvent =
                new DistributedChildEvent(connection.getUsername(), connection.getIpEndpoint());
        childDisconnectedListeners.forEach(listener -> listener.accept(childEvent));
        publishStateChanged();
        connection.close();
        updateStatusEventually();
    }

    private void parentCandidateDisconnected(ConnectionDisconnectedEvent eventData) {
        MessageConnection connection = (MessageConnection) eventData.connection();
        Subscription subscription = parentCandidateDisconnectSubscriptions.remove(connection);
        if (subscription != null) {
            subscription.close();
        }
        diagnostic.debug("Parent candidate connection to " + connection.getUsername()
                + " (" + connection.getIpEndpoint() + ") disconnected: "
                + eventData.message() + " (type: "
                + connection.getType() + ", id: "
                + connection.getId() + ")");
        connection.close();
    }

    private void parentDisconnected(ConnectionDisconnectedEvent eventData) {
        MessageConnection connection = (MessageConnection) eventData.connection();
        Subscription subscription = parentDisconnectSubscription;
        parentDisconnectSubscription = null;
        if (subscription != null) {
            subscription.close();
        }
        diagnostic.debug("Parent connection to " + connection.getUsername() + " ("
                + connection.getIpEndpoint() + ") disconnected: "
                + eventData.message() + " (type: "
                + connection.getType() + ", id: "
                + connection.getId() + ")");
        diagnostic.info("Parent connection to " + connection.getUsername() + " ("
                + connection.getIpEndpoint() + ") disconnected"
                + (eventData.message() == null ? "." : ": " + eventData.message()) + ".");
        DistributedParentEvent parentEvent = new DistributedParentEvent(
                connection.getUsername(), connection.getIpEndpoint(), parentBranchLevel, parentBranchRoot);
        parentDisconnectedListeners.forEach(listener -> listener.accept(parentEvent));
        parentConnection = null;
        parentBranchLevel = 0;
        parentBranchRoot = "";
        publishStateChanged();
        connection.close();
        // On a thread of its own: this runs from the dying connection's own
        // disconnect handler, and re-parenting negotiates with every remaining
        // candidate before it returns.
        List<PeerEndpoint> candidates = parentCandidates;
        scheduler.executor().execute(() -> {
            try {
                addParentConnection(candidates);
            } catch (Throwable failure) {
                diagnostic.debug("Failed to re-establish a parent connection: " + Failures.message(failure), failure);
            }
        });
    }

    private void updateStatusEventually() {
        if (lastStatusTimestamp != null
                && lastStatusTimestamp.plusMillis(STATUS_AGE_LIMIT).isBefore(Instant.now())) {
            diagnostic.debug("Distributed status age exceeds limit of " + STATUS_AGE_LIMIT + "ms, forcing an update");
            // Dispatched: this runs from the parent connection's read loop —
            // BRANCH_LEVEL and BRANCH_ROOT land here — and the update is a
            // blocking server write. The C# source fires and forgets the same
            // call; inline, a stalled server socket blocked the loop carrying
            // every inbound distributed search.
            scheduler.dispatch(
                    this::updateStatus,
                    failure -> diagnostic.debug(
                            "Failed to force a distributed status update: " + Failures.message(failure)));
        }
        ScheduledFuture<?> next = scheduler.schedule(this::updateStatus, STATUS_DEBOUNCE_TIME, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> prior = statusDebounce.getAndSet(next);
        if (prior != null) {
            prior.cancel(false);
        }
    }

    private boolean isEnabled() {
        return options.get().enableDistributedNetwork();
    }

    private boolean isAcceptingChildren() {
        return options.get().acceptDistributedChildren();
    }

    private String rejectionMessage(String username, InetSocketAddress endpoint) {
        return "Inbound child connection to " + username + " (" + endpoint
                + ") rejected: enabled " + isEnabled()
                + "; has parent: " + hasParent()
                + "; is branch root: " + isBranchRoot()
                + "; children: " + children.size() + "/" + getChildLimit();
    }

    private void publishChildAdded(MessageConnection connection) {
        DistributedChildEvent eventData =
                new DistributedChildEvent(connection.getUsername(), connection.getIpEndpoint());
        childAddedListeners.forEach(listener -> listener.accept(eventData));
    }

    private void publishStateChanged() {
        // Snapshotting the network builds two lists and walks every child, and
        // this runs from the message path — branch level, branch root and
        // parent changes all land here. With nobody listening it was pure
        // garbage; the sibling publish* methods already forEach over an empty
        // list for free, but only because they have a payload to hand.
        if (stateChangedListeners.isEmpty()) {
            return;
        }

        DistributedNetworkInfo info = new DistributedNetworkInfo(
                getAverageBroadcastLatency(),
                getBranchLevel(),
                getBranchRoot(),
                isBranchRoot(),
                getChildLimit(),
                canAcceptChildren(),
                getChildren().stream()
                        .map(child -> new DistributedPeer(child.username(), child.ipEndpoint()))
                        .toList(),
                new DistributedPeer(getParent().username(), getParent().ipEndpoint()),
                hasParent());
        stateChangedListeners.forEach(listener -> listener.accept(info));
    }

    private void publishDiagnostic(DiagnosticMessage eventData) {
        diagnosticListeners.forEach(listener -> listener.accept(eventData));
    }

    private void subscribeToChildDisconnect(MessageConnection connection) {
        Subscription subscription =
                connection.subscribe(TransportConnection.Kind.DISCONNECTED, childDisconnectedListener);
        Subscription previous = childDisconnectSubscriptions.put(connection, subscription);
        if (previous != null) {
            previous.close();
        }
    }

    private void subscribeToParentCandidateDisconnect(MessageConnection connection) {
        Subscription subscription =
                connection.subscribe(TransportConnection.Kind.DISCONNECTED, parentCandidateDisconnectedListener);
        Subscription previous = parentCandidateDisconnectSubscriptions.put(connection, subscription);
        if (previous != null) {
            previous.close();
        }
    }

    private static byte[] concatenate(byte[]... arrays) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (byte[] array : arrays) {
            output.writeBytes(array);
        }
        return output.toByteArray();
    }

    private static CancellationSignal token(CancellationSignal token) {
        return token == null ? CancellationSignal.none() : token;
    }

    private record ParentCandidate(MessageConnection connection, int branchLevel, String branchRoot) {}

    private record BranchInformation(int level, String root) {}

    private static final class LinkedCancellation implements AutoCloseable {
        private final CancellationController source = new CancellationController();
        private final CancellationSubscription registration;

        private LinkedCancellation(CancellationSignal parent) {
            registration = DistributedNetwork.token(parent).register(source::cancel);
        }

        private CancellationSignal token() {
            return source.getSignal();
        }

        private void cancel() {
            source.cancel();
        }

        @Override
        public void close() {
            registration.close();
            source.close();
        }
    }
}
