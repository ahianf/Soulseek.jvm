// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.CancellationRegistration;
import dev.slsk.CancellationToken;
import dev.slsk.CancellationTokenSource;
import dev.slsk.DistributedNetworkInfo;
import dev.slsk.DistributedPeer;
import dev.slsk.SoulseekClientStates;
import dev.slsk.common.Constants;
import dev.slsk.common.WaitKey;
import dev.slsk.diagnostics.DiagnosticEventArgs;
import dev.slsk.diagnostics.DiagnosticEventListener;
import dev.slsk.diagnostics.DiagnosticSink;
import dev.slsk.diagnostics.FilteringDiagnosticSink;
import dev.slsk.eventargs.DistributedChildEventArgs;
import dev.slsk.eventargs.DistributedParentEventArgs;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;
import dev.slsk.messaging.handlers.DistributedMessageHandler;
import dev.slsk.messaging.messages.AcceptChildrenCommand;
import dev.slsk.messaging.messages.BranchLevelCommand;
import dev.slsk.messaging.messages.BranchRootCommand;
import dev.slsk.messaging.messages.ConnectToPeerRequest;
import dev.slsk.messaging.messages.ConnectToPeerResponse;
import dev.slsk.messaging.messages.DistributedBranchLevel;
import dev.slsk.messaging.messages.DistributedBranchRoot;
import dev.slsk.messaging.messages.EmbeddedMessage;
import dev.slsk.messaging.messages.HaveNoParentsCommand;
import dev.slsk.messaging.messages.PeerInit;
import dev.slsk.messaging.messages.PierceFirewall;
import dev.slsk.network.tcp.Connection;
import dev.slsk.network.tcp.ConnectionDisconnectedEventArgs;
import dev.slsk.network.tcp.ConnectionEventListener;
import dev.slsk.network.tcp.ConnectionState;
import dev.slsk.network.tcp.ConnectionTypes;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** Manages distributed-network parent and child connections. */
public final class DefaultDistributedConnectionManager implements DistributedConnectionManager {
    static final int STATUS_AGE_LIMIT = 300_000;
    static final int STATUS_DEBOUNCE_TIME = 5_000;
    static final int WATCHDOG_TIME = 900_000;
    static final double LATENCY_ALPHA = 0.005d;

    private final DistributedConnectionManagerClient client;
    private final ConnectionFactory connectionFactory;
    private final DiagnosticSink diagnostic;
    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> watchdog;
    private final AtomicReference<ScheduledFuture<?>> statusDebounce = new AtomicReference<>();
    private final AtomicBoolean parentConnecting = new AtomicBoolean();
    private final AtomicBoolean statusUpdating = new AtomicBoolean();
    private final AtomicBoolean disposed = new AtomicBoolean();
    private final ConcurrentHashMap<String, CompletableFuture<MessageConnection>> childConnections =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, InetSocketAddress> children = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CancellationTokenSource> pendingInboundIndirectConnections =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, String> pendingSolicitations = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<DistributedManagerEventListener<DistributedChildEventArgs>> childAddedListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<DistributedManagerEventListener<DistributedChildEventArgs>>
            childDisconnectedListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<DistributedManagerEventListener<Void>> demotedListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<DistributedManagerEventListener<DistributedParentEventArgs>>
            parentAdoptedListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<DistributedManagerEventListener<DistributedParentEventArgs>>
            parentDisconnectedListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<DistributedManagerEventListener<Void>> promotedListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<DistributedManagerEventListener<DistributedNetworkInfo>> stateChangedListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<DiagnosticEventListener> diagnosticListeners = new CopyOnWriteArrayList<>();
    private final ConnectionEventListener<ConnectionDisconnectedEventArgs> parentCandidateDisconnectedListener =
            this::parentCandidateDisconnected;
    private final ConnectionEventListener<ConnectionDisconnectedEventArgs> parentDisconnectedListener =
            this::parentDisconnected;
    private final ConnectionEventListener<ConnectionDisconnectedEventArgs> childDisconnectedListener =
            this::childDisconnected;
    private final MessageConnectionEventListener<MessageEventArgs> parentInitializationListener =
            this::handleParentCandidateMessage;

    private volatile Double averageBroadcastLatency;
    private volatile boolean branchRootNode;
    private volatile String lastStatus;
    private volatile Instant lastStatusTimestamp;
    private volatile int parentBranchLevel;
    private volatile String parentBranchRoot = "";
    private volatile List<PeerEndpoint> parentCandidates = List.of();
    private volatile MessageConnection parentConnection;

    /** Creates a manager with default collaborators. */
    public DefaultDistributedConnectionManager(DistributedConnectionManagerClient client) {
        this(client, null, null);
    }

    /** Creates a manager. */
    public DefaultDistributedConnectionManager(
            DistributedConnectionManagerClient client,
            ConnectionFactory connectionFactory,
            DiagnosticSink diagnosticFactory) {
        this.client = Objects.requireNonNull(client, "client");
        this.connectionFactory = connectionFactory == null ? new DefaultConnectionFactory() : connectionFactory;
        diagnostic = diagnosticFactory == null
                ? new FilteringDiagnosticSink(client.getOptions().getMinimumDiagnosticLevel(), this::raiseDiagnostic)
                : diagnosticFactory;
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "soulseek-distributed-status");
            thread.setDaemon(true);
            return thread;
        });
        watchdog = scheduler.scheduleAtFixedRate(
                this::watchdogElapsed, WATCHDOG_TIME, WATCHDOG_TIME, TimeUnit.MILLISECONDS);
    }

    @Override
    public void addChildAddedListener(DistributedManagerEventListener<DistributedChildEventArgs> listener) {
        childAddedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeChildAddedListener(DistributedManagerEventListener<DistributedChildEventArgs> listener) {
        childAddedListeners.remove(listener);
    }

    @Override
    public void addChildDisconnectedListener(DistributedManagerEventListener<DistributedChildEventArgs> listener) {
        childDisconnectedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeChildDisconnectedListener(DistributedManagerEventListener<DistributedChildEventArgs> listener) {
        childDisconnectedListeners.remove(listener);
    }

    @Override
    public void addDemotedFromBranchRootListener(DistributedManagerEventListener<Void> listener) {
        demotedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeDemotedFromBranchRootListener(DistributedManagerEventListener<Void> listener) {
        demotedListeners.remove(listener);
    }

    @Override
    public void addDiagnosticGeneratedListener(DiagnosticEventListener listener) {
        diagnosticListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeDiagnosticGeneratedListener(DiagnosticEventListener listener) {
        diagnosticListeners.remove(listener);
    }

    @Override
    public void addParentAdoptedListener(DistributedManagerEventListener<DistributedParentEventArgs> listener) {
        parentAdoptedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeParentAdoptedListener(DistributedManagerEventListener<DistributedParentEventArgs> listener) {
        parentAdoptedListeners.remove(listener);
    }

    @Override
    public void addParentDisconnectedListener(DistributedManagerEventListener<DistributedParentEventArgs> listener) {
        parentDisconnectedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeParentDisconnectedListener(DistributedManagerEventListener<DistributedParentEventArgs> listener) {
        parentDisconnectedListeners.remove(listener);
    }

    @Override
    public void addPromotedToBranchRootListener(DistributedManagerEventListener<Void> listener) {
        promotedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removePromotedToBranchRootListener(DistributedManagerEventListener<Void> listener) {
        promotedListeners.remove(listener);
    }

    @Override
    public void addStateChangedListener(DistributedManagerEventListener<DistributedNetworkInfo> listener) {
        stateChangedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeStateChangedListener(DistributedManagerEventListener<DistributedNetworkInfo> listener) {
        stateChangedListeners.remove(listener);
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
        String value = hasParent() ? parentBranchRoot : client.getUsername();
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
        return client.getOptions().getDistributedChildLimit();
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
        return current != null && current.getState() == ConnectionState.CONNECTED;
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
                : new PeerEndpoint(current.getUsername(), current.getIpEndPoint());
    }

    @Override
    public Map<Integer, String> getPendingSolicitations() {
        return Map.copyOf(pendingSolicitations);
    }

    @Override
    public CompletableFuture<Void> addOrUpdateChildConnectionAsync(String username, Connection incomingConnection) {
        Objects.requireNonNull(incomingConnection, "incomingConnection");
        if (!canAcceptChildren()) {
            diagnostic.debug(rejectionMessage(username, incomingConnection.getIpEndPoint()));
            incomingConnection.close();
            return updateStatusAsync();
        }

        CompletableFuture<MessageConnection> future = childConnections.compute(username, (key, cached) -> {
            return invoke(() -> establishDirectChild(username, incomingConnection, cached));
        });
        return future.handle((connection, failure) -> {
            if (failure != null) {
                Throwable cause = unwrap(failure);
                String message = "Failed to establish an inbound direct child connection "
                        + "to " + username + " ("
                        + incomingConnection.getIpEndPoint() + "): "
                        + message(cause);
                diagnostic.debug(message + " (type: "
                        + incomingConnection.getType() + ", id: "
                        + incomingConnection.getId() + ")");
                diagnostic.debug("Purging child connection cache of failed connection to "
                        + username + " ("
                        + incomingConnection.getIpEndPoint() + ")");
                childConnections.remove(username);
                throw new CompletionException(new ConnectionException(message, cause));
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> addParentConnectionAsync(Iterable<PeerEndpoint> candidates) {
        if (!isEnabled()) {
            diagnostic.debug("Parent connection solicitation ignored; distributed " + "network is not enabled.");
            return CompletableFuture.completedFuture(null);
        }
        SoulseekClientStates state = client.getState();
        if (state.hasFlag(SoulseekClientStates.DISCONNECTED) || state.hasFlag(SoulseekClientStates.DISCONNECTING)) {
            return CompletableFuture.completedFuture(null);
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
            return updateStatusAsync();
        }
        if (!parentConnecting.compareAndSet(false, true)) {
            diagnostic.debug("Parent connection solicitation ignored; already in the "
                    + "process of establishing a connection.");
            return CompletableFuture.completedFuture(null);
        }

        diagnostic.info("Attempting to establish a new parent connection from " + snapshot.size() + " candidates");
        diagnostic.debug("Parent candidates: "
                + String.join(
                        ", ", snapshot.stream().map(PeerEndpoint::username).toList()));
        CancellationTokenSource cancellation = new CancellationTokenSource();
        List<CompletableFuture<ParentCandidate>> tasks = snapshot.stream()
                .map(candidate -> getParentCandidateConnectionAsync(
                        candidate.username(), candidate.ipEndPoint(), cancellation.getToken()))
                .toList();

        CompletableFuture<Void> settled = CompletableFuture.allOf(
                tasks.stream().map(task -> task.exceptionally(failure -> null)).toArray(CompletableFuture[]::new));
        return settled.thenCompose(ignored -> {
                    List<ParentCandidate> successful = tasks.stream()
                            .filter(task -> !task.isCompletedExceptionally() && !task.isCancelled())
                            .map(CompletableFuture::join)
                            .filter(Objects::nonNull)
                            .filter(candidate -> candidate.connection().getState() == ConnectionState.CONNECTED)
                            .sorted(Comparator.comparingInt(ParentCandidate::branchLevel))
                            .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
                    if (successful.isEmpty()) {
                        diagnostic.warning("Failed to connect to any of the available parent " + "candidates");
                        return CompletableFuture.completedFuture(null);
                    }

                    diagnostic.debug("Successfully established " + successful.size() + " connections.");
                    ParentCandidate selected = successful.removeFirst();
                    parentConnection = selected.connection();
                    parentBranchLevel = selected.branchLevel();
                    parentBranchRoot = selected.branchRoot();
                    diagnostic.debug("Selected " + parentConnection.getUsername()
                            + " as the best connection; branch root: "
                            + parentBranchRoot + ", branch level: "
                            + parentBranchLevel);
                    parentConnection.addDisconnectedListener(parentDisconnectedListener);
                    parentConnection.removeDisconnectedListener(parentCandidateDisconnectedListener);
                    DistributedMessageHandler handler = client.getDistributedMessageHandler();
                    parentConnection.addMessageReadListener(handler::handleMessageRead);
                    parentConnection.addMessageWrittenListener(handler::handleMessageWritten);
                    diagnostic.debug("Parent connection to " + parentConnection.getUsername()
                            + " (" + parentConnection.getIpEndPoint()
                            + ") established. (type: " + parentConnection.getType()
                            + ", id: " + parentConnection.getId() + ")");
                    diagnostic.info("Adopted parent connection to "
                            + parentConnection.getUsername() + " ("
                            + parentConnection.getIpEndPoint() + ")");
                    demoteFromBranchRoot();
                    DistributedParentEventArgs eventArgs = new DistributedParentEventArgs(
                            parentConnection.getUsername(),
                            parentConnection.getIpEndPoint(),
                            parentBranchLevel,
                            parentBranchRoot);
                    parentAdoptedListeners.forEach(listener -> listener.handle(this, eventArgs));
                    raiseStateChanged();

                    parentCandidates = successful.stream()
                            .map(candidate -> new PeerEndpoint(
                                    candidate.connection().getUsername(),
                                    candidate.connection().getIpEndPoint()))
                            .toList();
                    diagnostic.debug("Connected parent candidates not selected: "
                            + (parentCandidates.isEmpty()
                                    ? "<none>"
                                    : String.join(
                                            ", ",
                                            parentCandidates.stream()
                                                    .map(PeerEndpoint::username)
                                                    .toList())));
                    successful.forEach(candidate -> {
                        diagnostic.debug("Disconnecting parent candidate connection to "
                                + candidate.connection().getUsername() + " ("
                                + candidate.connection().getIpEndPoint() + ")");
                        candidate.connection().disconnect("Not selected.");
                        candidate.connection().close();
                    });
                    return updateStatusAsync().thenCompose(value -> broadcastMessageAsync(getBranchInformation()));
                })
                .whenComplete((ignored, failure) -> {
                    cancellation.close();
                    parentConnecting.set(false);
                    updateStatusAsync();
                });
    }

    @Override
    public CompletableFuture<Void> broadcastMessageAsync(byte[] bytes, CancellationToken cancellationToken) {
        long started = System.nanoTime();
        CancellationToken effectiveToken = token(cancellationToken);
        List<CompletableFuture<Void>> writes = childConnections.values().stream()
                .map(future -> future.thenCompose(connection -> {
                            if (connection == null || connection.getState() != ConnectionState.CONNECTED) {
                                return CompletableFuture.completedFuture(null);
                            }
                            return connection.writeAsync(bytes, effectiveToken).exceptionally(failure -> {
                                connection.disconnect("Broadcast failure: " + message(unwrap(failure)));
                                return null;
                            });
                        })
                        .exceptionally(failure -> null))
                .toList();
        return CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new)).thenRun(() -> {
            double elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            Double current = averageBroadcastLatency;
            averageBroadcastLatency = current == null ? elapsed : ((elapsed - current) * LATENCY_ALPHA) + current;
        });
    }

    @Override
    public void demoteFromBranchRoot() {
        if (branchRootNode) {
            branchRootNode = false;
            diagnostic.info("Demoted from distributed branch root.");
            demotedListeners.forEach(listener -> listener.handle(this, null));
            raiseStateChanged();
        }
    }

    @Override
    public CompletableFuture<Void> getOrAddChildConnectionAsync(ConnectToPeerResponse response) {
        if (!canAcceptChildren()) {
            diagnostic.debug(rejectionMessage(response.getUsername(), response.getIpEndPoint()));
            return updateStatusAsync();
        }

        AtomicBoolean cached = new AtomicBoolean(true);
        CompletableFuture<MessageConnection> future = childConnections.computeIfAbsent(response.getUsername(), key -> {
            cached.set(false);
            return establishIndirectChild(response);
        });
        return future.handle((connection, failure) -> {
            if (failure == null) {
                if (cached.get()) {
                    diagnostic.debug("Child connection from " + response.getUsername()
                            + " (" + response.getIpEndPoint()
                            + ") for token " + response.getToken()
                            + " ignored; connection already exists.");
                }
                return null;
            }

            Throwable cause = unwrap(failure);
            String message = "Failed to establish an inbound indirect child connection "
                    + "to " + response.getUsername() + " ("
                    + response.getIpEndPoint() + "): " + message(cause);
            diagnostic.debug(message);
            if (!(cause instanceof CancellationException)) {
                diagnostic.debug("Purging child connection cache of failed connection to "
                        + response.getUsername() + " ("
                        + response.getIpEndPoint() + ").");
                CompletableFuture<MessageConnection> removedRecord = childConnections.remove(response.getUsername());
                if (removedRecord != null) {
                    removedRecord.handle((removed, ignored) -> {
                        if (removed != null && removed.getType().hasFlag(ConnectionTypes.DIRECT)) {
                            diagnostic.warning("Erroneously purged direct child connection "
                                    + "to " + response.getUsername()
                                    + " upon indirect failure");
                            childConnections.putIfAbsent(response.getUsername(), removedRecord);
                        }
                        return null;
                    });
                }
            }
            throw new CompletionException(new ConnectionException(message, cause));
        });
    }

    @Override
    public void promoteToBranchRoot() {
        if (!branchRootNode && !hasParent()) {
            branchRootNode = true;
            diagnostic.info("Promoted to distributed branch root.");
            promotedListeners.forEach(listener -> listener.handle(this, null));
            raiseStateChanged();
        }
    }

    @Override
    public void removeAndDisposeAll() {
        pendingSolicitations.clear();
        pendingInboundIndirectConnections.clear();
        MessageConnection parent = parentConnection;
        if (parent != null) {
            parent.close();
        }
        parentConnection = null;
        childConnections.forEach((username, future) -> {
            if (childConnections.remove(username, future)) {
                future.thenAccept(connection -> {
                    if (connection != null) {
                        connection.close();
                    }
                });
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
        updateStatusEventuallyAsync();
    }

    @Override
    public void setParentBranchRoot(String branchRoot) {
        parentBranchRoot = branchRoot;
        updateStatusEventuallyAsync();
    }

    @Override
    public CompletableFuture<Void> updateStatusAsync(CancellationToken cancellationToken) {
        SoulseekClientStates state = client.getState();
        if (!state.hasFlag(SoulseekClientStates.CONNECTED) || !state.hasFlag(SoulseekClientStates.LOGGED_IN)) {
            return CompletableFuture.completedFuture(null);
        }
        if (!statusUpdating.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }

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
            statusUpdating.set(false);
            return CompletableFuture.completedFuture(null);
        }

        diagnostic.debug("Status changed; " + status);
        byte[] payload = concatenate(
                new BranchLevelCommand(branchLevel).toByteArray(),
                new BranchRootCommand(branchRoot).toByteArray(),
                new AcceptChildrenCommand(accept).toByteArray(),
                new HaveNoParentsCommand(haveNoParents).toByteArray());
        return client.getServerConnection()
                .writeAsync(payload, token(cancellationToken))
                .handle((ignored, failure) -> {
                    if (failure == null) {
                        raiseStateChanged();
                        diagnostic.info("Updated distributed status; " + status);
                        lastStatus = status;
                        lastStatusTimestamp = Instant.now();
                    } else {
                        Throwable cause = unwrap(failure);
                        String message = "Failed to update distributed status: " + message(cause);
                        if (!client.getState().equals(SoulseekClientStates.DISCONNECTED)) {
                            diagnostic.warning(message, cause);
                        } else {
                            diagnostic.debug(message, cause);
                        }
                    }
                    statusUpdating.set(false);
                    return null;
                });
    }

    @Override
    public void close() {
        if (disposed.compareAndSet(false, true)) {
            watchdog.cancel(false);
            ScheduledFuture<?> debounce = statusDebounce.getAndSet(null);
            if (debounce != null) {
                debounce.cancel(false);
            }
            scheduler.shutdownNow();
            removeAndDisposeAll();
        }
    }

    byte[] getBranchInformation() {
        return concatenate(
                new DistributedBranchLevel(getBranchLevel()).toByteArray(),
                new DistributedBranchRoot(getBranchRoot()).toByteArray());
    }

    void watchdogElapsed() {
        SoulseekClientStates state = client.getState();
        if (isEnabled()
                && !hasParent()
                && !isBranchRoot()
                && state.hasFlag(SoulseekClientStates.CONNECTED)
                && state.hasFlag(SoulseekClientStates.LOGGED_IN)) {
            diagnostic.warning("No distributed parent connected.  Requesting a list of " + "candidates.");
            updateStatusAsync();
        }
    }

    void handleParentCandidateMessage(MessageConnection connection, MessageEventArgs eventArgs) {
        try {
            byte[] message = eventArgs.getMessage();
            MessageCode.Distributed code = new MessageReader<>(message, MessageCode.Distributed.class).readCode();
            switch (code) {
                case EMBEDDED_MESSAGE -> {
                    EmbeddedMessage embedded = EmbeddedMessage.fromByteArray(message);
                    if (embedded.getDistributedCode() == MessageCode.Distributed.SEARCH_REQUEST) {
                        client.getWaiter()
                                .complete(new WaitKey(Constants.WaitKey.SEARCH_REQUEST_MESSAGE, connection.getId()));
                    }
                }
                case SEARCH_REQUEST ->
                    client.getWaiter()
                            .complete(new WaitKey(Constants.WaitKey.SEARCH_REQUEST_MESSAGE, connection.getId()));
                case BRANCH_LEVEL ->
                    client.getWaiter()
                            .complete(
                                    new WaitKey(Constants.WaitKey.BRANCH_LEVEL_MESSAGE, connection.getId()),
                                    DistributedBranchLevel.fromByteArray(message)
                                            .getLevel());
                case BRANCH_ROOT ->
                    client.getWaiter()
                            .complete(
                                    new WaitKey(Constants.WaitKey.BRANCH_ROOT_MESSAGE, connection.getId()),
                                    DistributedBranchRoot.fromByteArray(message).getUsername());
                default -> {
                    // Source ignores all other distributed messages here.
                }
            }
        } catch (Throwable failure) {
            diagnostic.debug("Failed to handle message from parent candidate: " + message(failure), failure);
            connection.disconnect(message(failure));
            connection.close();
        }
    }

    private CompletableFuture<MessageConnection> establishDirectChild(
            String username, Connection incomingConnection, CompletableFuture<MessageConnection> cached) {
        diagnostic.debug("Inbound child connection to " + username + " ("
                + incomingConnection.getIpEndPoint()
                + ") accepted. (type: " + incomingConnection.getType()
                + ", id: " + incomingConnection.getId());
        MessageConnection connection = connectionFactory.getDistributedConnection(
                username,
                incomingConnection.getIpEndPoint(),
                client.getOptions().getDistributedConnectionOptions(),
                incomingConnection.handoffTcpClient());
        diagnostic.debug("Inbound child connection to " + username + " ("
                + connection.getIpEndPoint() + ") handed off. (old: "
                + incomingConnection.getId() + ", new: "
                + connection.getId() + ")");
        incomingConnection.close();
        connection.setType(ConnectionTypes.INBOUND.or(ConnectionTypes.DIRECT));
        attachChildMessageListeners(connection);
        connection.addDisconnectedListener((sender, args) -> sender.close());
        AtomicBoolean superseded = new AtomicBoolean();

        CompletableFuture<Void> prior = CompletableFuture.completedFuture(null);
        if (cached != null) {
            CancellationTokenSource pending = pendingInboundIndirectConnections.get(username);
            if (pending != null) {
                diagnostic.debug("Cancelling pending indirect child connection to " + username);
                pending.cancel();
            }
            prior = cached.handle((old, failure) -> {
                if (old != null) {
                    old.removeDisconnectedListener(childDisconnectedListener);
                    diagnostic.debug("Superseding existing child connection to "
                            + username + " (" + old.getIpEndPoint()
                            + ") (old: " + incomingConnection.getId()
                            + ", new: " + connection.getId());
                    old.disconnect("Superseded.");
                    old.close();
                    superseded.set(true);
                }
                return null;
            });
        }
        return prior.thenCompose(ignored -> {
            try {
                connection.startReadingContinuously();
            } catch (Throwable failure) {
                connection.close();
                return CompletableFuture.failedFuture(failure);
            }
            return connection
                    .writeAsync(getBranchInformation())
                    .thenApply(value -> {
                        connection.addDisconnectedListener(childDisconnectedListener);
                        children.put(username, connection.getIpEndPoint());
                        diagnostic.debug("Child connection to " + connection.getUsername()
                                + " (" + connection.getIpEndPoint()
                                + ") established. (type: "
                                + connection.getType() + ", id: "
                                + connection.getId() + ")");
                        diagnostic.info((superseded.get() ? "Updated" : "Added")
                                + " child connection to "
                                + connection.getUsername() + " ("
                                + connection.getIpEndPoint() + ")");
                        if (!superseded.get()) {
                            raiseChildAdded(connection);
                            raiseStateChanged();
                        }
                        updateStatusEventuallyAsync();
                        return connection;
                    })
                    .whenComplete((value, failure) -> {
                        if (failure != null) {
                            connection.close();
                        }
                    });
        });
    }

    private CompletableFuture<MessageConnection> establishIndirectChild(ConnectToPeerResponse response) {
        diagnostic.debug("Attempting inbound indirect child connection to "
                + response.getUsername() + " (" + response.getIpEndPoint()
                + ") for token " + response.getToken());
        MessageConnection connection = connectionFactory.getDistributedConnection(
                response.getUsername(),
                response.getIpEndPoint(),
                client.getOptions().getDistributedConnectionOptions());
        connection.setType(ConnectionTypes.INBOUND.or(ConnectionTypes.INDIRECT));
        attachChildMessageListeners(connection);
        connection.addDisconnectedListener((sender, args) -> sender.close());
        CancellationTokenSource cancellation = new CancellationTokenSource();
        pendingInboundIndirectConnections.put(response.getUsername(), cancellation);

        return connection
                .connectAsync(cancellation.getToken())
                .thenCompose(ignored -> connection.writeAsync(
                        new PierceFirewall(response.getToken()).toByteArray(), cancellation.getToken()))
                .thenCompose(ignored -> connection.writeAsync(getBranchInformation(), cancellation.getToken()))
                .handle((ignored, failure) -> {
                    pendingInboundIndirectConnections.remove(response.getUsername(), cancellation);
                    cancellation.close();
                    if (failure != null) {
                        connection.close();
                        throw new CompletionException(unwrap(failure));
                    }
                    connection.addDisconnectedListener(childDisconnectedListener);
                    children.put(response.getUsername(), connection.getIpEndPoint());
                    diagnostic.debug("Child connection to " + connection.getUsername() + " ("
                            + connection.getIpEndPoint()
                            + ") established. (type: " + connection.getType()
                            + ", id: " + connection.getId() + ")");
                    diagnostic.info("Added child connection to " + connection.getUsername() + " ("
                            + connection.getIpEndPoint() + ")");
                    raiseChildAdded(connection);
                    raiseStateChanged();
                    updateStatusEventuallyAsync();
                    return connection;
                });
    }

    private CompletableFuture<ParentCandidate> getParentCandidateConnectionAsync(
            String username, InetSocketAddress ipEndPoint, CancellationToken cancellationToken) {
        LinkedCancellation directCancellation = new LinkedCancellation(cancellationToken);
        LinkedCancellation indirectCancellation = new LinkedCancellation(cancellationToken);
        diagnostic.debug("Attempting simultaneous direct and indirect parent candidate " + "connections to " + username
                + " (" + ipEndPoint + ")");
        CompletableFuture<MessageConnection> direct =
                getParentCandidateConnectionDirectAsync(username, ipEndPoint, directCancellation.token());
        CompletableFuture<MessageConnection> indirect =
                getParentCandidateConnectionIndirectAsync(username, indirectCancellation.token());

        return firstSuccessful(direct, indirect)
                .thenCompose(winner -> {
                    boolean directWon = winner.source() == direct;
                    MessageConnection connection = winner.value();
                    diagnostic.debug((directWon ? "Direct" : "Indirect")
                            + " parent candidate connection to " + username + " ("
                            + ipEndPoint
                            + ") established first, attempting to cancel "
                            + (directWon ? "indirect" : "direct") + " connection.");
                    (directWon ? indirectCancellation : directCancellation).cancel();
                    CompletableFuture<BranchInformation> initialization =
                            waitForParentCandidateConnectionInitializationAsync(connection, cancellationToken);
                    CompletableFuture<Void> negotiation;
                    try {
                        if (directWon) {
                            negotiation = connection.writeAsync(
                                    new PeerInit(
                                                    client.getUsername(),
                                                    Constants.ConnectionType.DISTRIBUTED,
                                                    client.getNextToken())
                                            .toByteArray(),
                                    token(cancellationToken));
                        } else {
                            connection.startReadingContinuously();
                            negotiation = CompletableFuture.completedFuture(null);
                        }
                    } catch (Throwable failure) {
                        negotiation = CompletableFuture.failedFuture(failure);
                    }
                    diagnostic.debug((directWon ? "Direct" : "Indirect")
                            + " parent candidate connection to " + username + " ("
                            + ipEndPoint + ") initialized.  Waiting for branch "
                            + "information and first search request. (id: "
                            + connection.getId() + ")");
                    return negotiation.thenCompose(ignored -> initialization).handle((branch, failure) -> {
                        directCancellation.close();
                        indirectCancellation.close();
                        if (failure != null) {
                            Throwable cause = unwrap(failure);
                            String message = "Failed to negotiate parent candidate "
                                    + "connection to " + username + " ("
                                    + ipEndPoint + "): " + message(cause);
                            diagnostic.debug(
                                    message + " (type: " + connection.getType() + ", id: " + connection.getId() + ")");
                            connection.close();
                            throw new CompletionException(new ConnectionException(message, cause));
                        }
                        diagnostic.debug("Parent candidate connection to " + username + " ("
                                + ipEndPoint + ") established. (type: "
                                + connection.getType() + ", id: "
                                + connection.getId() + ")");
                        return new ParentCandidate(connection, branch.level(), branch.root());
                    });
                })
                .handle((candidate, failure) -> {
                    if (failure != null) {
                        directCancellation.close();
                        indirectCancellation.close();
                        Throwable cause = unwrap(failure);
                        if (cause instanceof ConnectionException) {
                            throw new CompletionException(cause);
                        }
                        String message = "Failed to establish a direct or indirect parent "
                                + "candidate connection to " + username + " ("
                                + ipEndPoint + ")";
                        diagnostic.debug(message);
                        throw new CompletionException(new ConnectionException(message));
                    }
                    return candidate;
                });
    }

    private CompletableFuture<MessageConnection> getParentCandidateConnectionDirectAsync(
            String username, InetSocketAddress ipEndPoint, CancellationToken cancellationToken) {
        diagnostic.debug("Attempting direct parent candidate connection to " + username + " (" + ipEndPoint + ")");
        MessageConnection connection = connectionFactory.getDistributedConnection(
                username, ipEndPoint, client.getOptions().getDistributedConnectionOptions());
        connection.setType(ConnectionTypes.OUTBOUND.or(ConnectionTypes.DIRECT));
        connection.addDisconnectedListener(parentCandidateDisconnectedListener);
        return connection.connectAsync(cancellationToken).handle((ignored, failure) -> {
            if (failure != null) {
                diagnostic.debug("Failed to establish a direct parent candidate "
                        + "connection to " + username + " ("
                        + ipEndPoint + "): "
                        + message(unwrap(failure)));
                connection.close();
                throw new CompletionException(unwrap(failure));
            }
            diagnostic.debug("Direct parent candidate connection to " + username
                    + " (" + connection.getIpEndPoint()
                    + ") established. (type: " + connection.getType()
                    + ", id: " + connection.getId() + ")");
            return connection;
        });
    }

    private CompletableFuture<MessageConnection> getParentCandidateConnectionIndirectAsync(
            String username, CancellationToken cancellationToken) {
        int solicitationToken = client.getNextToken();
        diagnostic.debug(
                "Soliciting indirect parent candidate connection to " + username + " with token " + solicitationToken);
        pendingSolicitations.putIfAbsent(solicitationToken, username);
        return client.getServerConnection()
                .writeAsync(
                        new ConnectToPeerRequest(solicitationToken, username, Constants.ConnectionType.DISTRIBUTED),
                        cancellationToken)
                .thenCompose(ignored -> client.getWaiter()
                        .waitAsync(
                                new WaitKey(
                                        Constants.WaitKey.SOLICITED_DISTRIBUTED_CONNECTION,
                                        username,
                                        solicitationToken),
                                Connection.class,
                                client.getOptions()
                                        .getDistributedConnectionOptions()
                                        .getConnectTimeout(),
                                cancellationToken))
                .thenApply(accepted -> {
                    try {
                        MessageConnection connection = connectionFactory.getDistributedConnection(
                                username,
                                accepted.getIpEndPoint(),
                                client.getOptions().getDistributedConnectionOptions(),
                                accepted.handoffTcpClient());
                        diagnostic.debug("Indirect parent candidate connection to " + username
                                + " (" + accepted.getIpEndPoint()
                                + ") handed off. (old: " + accepted.getId()
                                + ", new: " + connection.getId() + ")");
                        connection.setType(ConnectionTypes.OUTBOUND.or(ConnectionTypes.INDIRECT));
                        connection.addDisconnectedListener(parentCandidateDisconnectedListener);
                        diagnostic.debug("Indirect parent candidate connection to " + username
                                + " (" + connection.getIpEndPoint()
                                + ") established. (type: "
                                + connection.getType() + ", id: "
                                + connection.getId() + ")");
                        return connection;
                    } finally {
                        accepted.close();
                    }
                })
                .handle((connection, failure) -> {
                    pendingSolicitations.remove(solicitationToken, username);
                    if (failure != null) {
                        diagnostic.debug("Failed to establish an indirect parent candidate "
                                + "connection to " + username + " with token "
                                + solicitationToken + ": "
                                + message(unwrap(failure)));
                        throw new CompletionException(unwrap(failure));
                    }
                    return connection;
                });
    }

    private CompletableFuture<BranchInformation> waitForParentCandidateConnectionInitializationAsync(
            MessageConnection connection, CancellationToken cancellationToken) {
        connection.addMessageReadListener(parentInitializationListener);
        CompletableFuture<Integer> branchLevel = client.getWaiter()
                .waitAsync(
                        new WaitKey(Constants.WaitKey.BRANCH_LEVEL_MESSAGE, connection.getId()),
                        Integer.class,
                        null,
                        token(cancellationToken));
        CompletableFuture<String> branchRoot = client.getWaiter()
                .waitAsync(
                        new WaitKey(Constants.WaitKey.BRANCH_ROOT_MESSAGE, connection.getId()),
                        String.class,
                        null,
                        token(cancellationToken));
        CompletableFuture<Void> search = client.getWaiter()
                .waitAsync(
                        new WaitKey(Constants.WaitKey.SEARCH_REQUEST_MESSAGE, connection.getId()),
                        null,
                        token(cancellationToken));
        return branchLevel
                .thenCombine(search, (level, ignored) -> level)
                .thenCompose(level -> {
                    if (level > 0) {
                        return branchRoot.thenApply(root -> new BranchInformation(level, root));
                    }
                    diagnostic.debug("Received branch level 0 from parent candidate "
                            + connection.getUsername()
                            + "; this user is a branch root.");
                    return CompletableFuture.completedFuture(new BranchInformation(level, connection.getUsername()));
                })
                .handle((branch, failure) -> {
                    connection.removeMessageReadListener(parentInitializationListener);
                    if (failure != null) {
                        connection.disconnect("One or more required messages was not received.");
                        throw new CompletionException(
                                new ConnectionException("Failed to retrieve branch info from parent "
                                        + "candidate connection to "
                                        + connection.getUsername() + " ("
                                        + connection.getIpEndPoint()
                                        + "); one or more required messages was not "
                                        + "received. (id: " + connection.getId() + ")"));
                    }
                    return branch;
                });
    }

    private void attachChildMessageListeners(MessageConnection connection) {
        DistributedMessageHandler handler = client.getDistributedMessageHandler();
        connection.addMessageReadListener(handler::handleChildMessageRead);
        connection.addMessageWrittenListener(handler::handleChildMessageWritten);
    }

    private void childDisconnected(Connection sender, ConnectionDisconnectedEventArgs eventArgs) {
        MessageConnection connection = (MessageConnection) sender;
        childConnections.remove(connection.getUsername());
        children.remove(connection.getUsername());
        diagnostic.debug("Child connection to " + connection.getUsername() + " ("
                + connection.getIpEndPoint() + ") disconnected: "
                + eventArgs.getMessage() + " (type: "
                + connection.getType() + ", id: "
                + connection.getId() + ")");
        diagnostic.info("Child connection to " + connection.getUsername() + " ("
                + connection.getIpEndPoint() + ") disconnected"
                + (eventArgs.getMessage() == null ? "." : ": " + eventArgs.getMessage()));
        DistributedChildEventArgs childEvent =
                new DistributedChildEventArgs(connection.getUsername(), connection.getIpEndPoint());
        childDisconnectedListeners.forEach(listener -> listener.handle(this, childEvent));
        raiseStateChanged();
        connection.close();
        updateStatusEventuallyAsync();
    }

    private void parentCandidateDisconnected(Connection sender, ConnectionDisconnectedEventArgs eventArgs) {
        MessageConnection connection = (MessageConnection) sender;
        diagnostic.debug("Parent candidate connection to " + connection.getUsername()
                + " (" + connection.getIpEndPoint() + ") disconnected: "
                + eventArgs.getMessage() + " (type: "
                + connection.getType() + ", id: "
                + connection.getId() + ")");
        connection.close();
    }

    private void parentDisconnected(Connection sender, ConnectionDisconnectedEventArgs eventArgs) {
        MessageConnection connection = (MessageConnection) sender;
        diagnostic.debug("Parent connection to " + connection.getUsername() + " ("
                + connection.getIpEndPoint() + ") disconnected: "
                + eventArgs.getMessage() + " (type: "
                + connection.getType() + ", id: "
                + connection.getId() + ")");
        diagnostic.info("Parent connection to " + connection.getUsername() + " ("
                + connection.getIpEndPoint() + ") disconnected"
                + (eventArgs.getMessage() == null ? "." : ": " + eventArgs.getMessage()) + ".");
        DistributedParentEventArgs parentEvent = new DistributedParentEventArgs(
                connection.getUsername(), connection.getIpEndPoint(), parentBranchLevel, parentBranchRoot);
        parentDisconnectedListeners.forEach(listener -> listener.handle(this, parentEvent));
        parentConnection = null;
        parentBranchLevel = 0;
        parentBranchRoot = "";
        raiseStateChanged();
        connection.close();
        addParentConnectionAsync(parentCandidates).exceptionally(failure -> null);
    }

    private CompletableFuture<Void> updateStatusEventuallyAsync() {
        if (lastStatusTimestamp != null
                && lastStatusTimestamp.plusMillis(STATUS_AGE_LIMIT).isBefore(Instant.now())) {
            diagnostic.debug("Distributed status age exceeds limit of " + STATUS_AGE_LIMIT + "ms, forcing an update");
            updateStatusAsync();
        }
        ScheduledFuture<?> next =
                scheduler.schedule(() -> updateStatusAsync(), STATUS_DEBOUNCE_TIME, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> prior = statusDebounce.getAndSet(next);
        if (prior != null) {
            prior.cancel(false);
        }
        return CompletableFuture.completedFuture(null);
    }

    private boolean isEnabled() {
        return client.getOptions().isEnableDistributedNetwork();
    }

    private boolean isAcceptingChildren() {
        return client.getOptions().isAcceptDistributedChildren();
    }

    private String rejectionMessage(String username, InetSocketAddress endpoint) {
        return "Inbound child connection to " + username + " (" + endpoint
                + ") rejected: enabled " + isEnabled()
                + "; has parent: " + hasParent()
                + "; is branch root: " + isBranchRoot()
                + "; children: " + children.size() + "/" + getChildLimit();
    }

    private void raiseChildAdded(MessageConnection connection) {
        DistributedChildEventArgs eventArgs =
                new DistributedChildEventArgs(connection.getUsername(), connection.getIpEndPoint());
        childAddedListeners.forEach(listener -> listener.handle(this, eventArgs));
    }

    private void raiseStateChanged() {
        DistributedNetworkInfo info = new DistributedNetworkInfo(
                getAverageBroadcastLatency(),
                getBranchLevel(),
                getBranchRoot(),
                isBranchRoot(),
                getChildLimit(),
                canAcceptChildren(),
                getChildren().stream()
                        .map(child -> new DistributedPeer(child.username(), child.ipEndPoint()))
                        .toList(),
                new DistributedPeer(getParent().username(), getParent().ipEndPoint()),
                hasParent());
        stateChangedListeners.forEach(listener -> listener.handle(this, info));
    }

    private void raiseDiagnostic(DiagnosticEventArgs eventArgs) {
        diagnosticListeners.forEach(listener -> listener.handle(this, eventArgs));
    }

    private static byte[] concatenate(byte[]... arrays) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (byte[] array : arrays) {
            output.writeBytes(array);
        }
        return output.toByteArray();
    }

    private static <T> CompletableFuture<T> invoke(Supplier<CompletableFuture<T>> supplier) {
        try {
            return supplier.get();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static <T> CompletableFuture<Winner<T>> firstSuccessful(
            CompletableFuture<T> first, CompletableFuture<T> second) {
        CompletableFuture<Winner<T>> result = new CompletableFuture<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        first.whenComplete((value, firstFailure) -> {
            if (firstFailure == null) {
                result.complete(new Winner<>(value, first));
            } else if (failure.getAndSet(unwrap(firstFailure)) != null) {
                result.completeExceptionally(unwrap(firstFailure));
            }
        });
        second.whenComplete((value, secondFailure) -> {
            if (secondFailure == null) {
                result.complete(new Winner<>(value, second));
            } else if (failure.getAndSet(unwrap(secondFailure)) != null) {
                result.completeExceptionally(unwrap(secondFailure));
            }
        });
        return result;
    }

    private static CancellationToken token(CancellationToken token) {
        return token == null ? CancellationToken.none() : token;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String message(Throwable failure) {
        return failure.getMessage() == null ? "" : failure.getMessage();
    }

    private record Winner<T>(T value, CompletableFuture<T> source) {}

    private record ParentCandidate(MessageConnection connection, int branchLevel, String branchRoot) {}

    private record BranchInformation(int level, String root) {}

    private static final class LinkedCancellation implements AutoCloseable {
        private final CancellationTokenSource source = new CancellationTokenSource();
        private final CancellationRegistration registration;

        private LinkedCancellation(CancellationToken parent) {
            registration = DefaultDistributedConnectionManager.token(parent).register(source::cancel);
        }

        private CancellationToken token() {
            return source.getToken();
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
