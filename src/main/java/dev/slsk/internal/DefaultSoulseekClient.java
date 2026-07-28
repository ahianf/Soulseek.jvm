// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static dev.slsk.internal.ClientSupport.acquirePermit;
import static dev.slsk.internal.ClientSupport.failureMessage;
import static dev.slsk.internal.ClientSupport.mapClientFailure;
import static dev.slsk.internal.ClientSupport.requireNonEmpty;
import static dev.slsk.internal.ClientSupport.requireText;
import static dev.slsk.internal.ClientSupport.unwrap;

import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.AddressException;
import dev.slsk.exceptions.KickedFromServerException;
import dev.slsk.exceptions.ListenException;
import dev.slsk.exceptions.LoginRejectedException;
import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferReportedFailedException;
import dev.slsk.internal.common.DefaultWaiter;
import dev.slsk.internal.common.IOAdapter;
import dev.slsk.internal.common.Scheduler;
import dev.slsk.internal.common.TokenBucket;
import dev.slsk.internal.common.TokenFactory;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.diagnostics.FilteringDiagnosticSink;
import dev.slsk.internal.diagnostics.GlobalDiagnostic;
import dev.slsk.internal.events.BrowseProgressUpdatedEvent;
import dev.slsk.internal.events.DownloadDeniedEvent;
import dev.slsk.internal.events.DownloadFailedEvent;
import dev.slsk.internal.events.SoulseekClientDisconnectedEvent;
import dev.slsk.internal.events.SoulseekClientStateChangedEvent;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.handlers.DefaultDistributedMessageHandler;
import dev.slsk.internal.messaging.handlers.DefaultPeerMessageHandler;
import dev.slsk.internal.messaging.handlers.DefaultServerMessageHandler;
import dev.slsk.internal.messaging.handlers.DistributedMessageHandler;
import dev.slsk.internal.messaging.handlers.DistributedMessageHandlerClient;
import dev.slsk.internal.messaging.handlers.PeerMessageHandler;
import dev.slsk.internal.messaging.handlers.PeerMessageHandlerClient;
import dev.slsk.internal.messaging.handlers.ServerMessageEvent;
import dev.slsk.internal.messaging.handlers.ServerMessageHandler;
import dev.slsk.internal.messaging.handlers.ServerMessageHandlerClient;
import dev.slsk.internal.messaging.messages.LoginRequest;
import dev.slsk.internal.messaging.messages.LoginResponse;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.messaging.messages.PrivateRoomToggle;
import dev.slsk.internal.messaging.messages.SetListenPortCommand;
import dev.slsk.internal.network.ConnectionFactory;
import dev.slsk.internal.network.DefaultConnectionFactory;
import dev.slsk.internal.network.DefaultDistributedConnectionManager;
import dev.slsk.internal.network.DefaultListenerHandler;
import dev.slsk.internal.network.DefaultPeerConnectionManager;
import dev.slsk.internal.network.DistributedConnectionManager;
import dev.slsk.internal.network.DistributedConnectionManagerClient;
import dev.slsk.internal.network.ListenerHandler;
import dev.slsk.internal.network.ListenerHandlerClient;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.PeerConnectionManager;
import dev.slsk.internal.network.PeerConnectionManagerClient;
import dev.slsk.internal.network.PeerEndpoint;
import dev.slsk.internal.network.tcp.Listener;
import dev.slsk.internal.network.tcp.SocketListener;
import dev.slsk.internal.options.BrowseOptions;
import dev.slsk.internal.options.BrowseProgress;
import dev.slsk.internal.options.ConnectionOptions;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.options.SoulseekClientOptionsPatch;
import dev.slsk.internal.search.DefaultSearchResponder;
import dev.slsk.internal.search.SearchInternal;
import dev.slsk.internal.search.SearchResponder;
import dev.slsk.internal.search.SearchResponderClient;
import dev.slsk.internal.transfer.TransferInternal;
import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * A client for the Soulseek file-sharing network.
 */
final class DefaultSoulseekClient extends ClientOperations
        implements SoulseekClient,
                ClientContext,
                DistributedConnectionManagerClient,
                DistributedMessageHandlerClient,
                ListenerHandlerClient,
                PeerConnectionManagerClient,
                PeerMessageHandlerClient,
                SearchResponderClient,
                ServerMessageHandlerClient {

    private static final int MAJOR_VERSION = 170;
    private static final String DEFAULT_ADDRESS = "server.slsknet.org";
    private static final int DEFAULT_PORT = 2271;
    private static volatile boolean raiseEventsAsynchronously;

    volatile SoulseekClientOptions options;
    private final int minorVersion;
    final Waiter waiter;
    private final TokenFactory tokenFactory;
    private final Semaphore searchSemaphore;
    final Semaphore stateSemaphore = new Semaphore(1);
    private final Semaphore globalDownloadSemaphore;
    private final Semaphore globalUploadSemaphore;
    private final Semaphore uploadSemaphoreSyncRoot = new Semaphore(1);
    private final IOAdapter ioAdapter;
    final TokenBucket uploadTokenBucket;
    final TokenBucket downloadTokenBucket;
    final ConnectionFactory connectionFactory;
    final ListenerHandler listenerHandler;
    final SearchResponder searchResponder;
    private final PeerMessageHandler peerMessageHandler;
    private final DistributedMessageHandler distributedMessageHandler;
    final PeerConnectionManager peerConnectionManager;
    final DistributedConnectionManager distributedConnectionManager;
    private final ServerMessageHandler serverMessageHandler;
    final DiagnosticSink diagnostic;
    volatile ClientListenerFactory clientListenerFactory = SocketListener::new;
    private final AtomicBoolean closed = new AtomicBoolean();
    /**
     * The client's single timer thread. Every component that needs delayed or
     * periodic work shares it: the waiter, both token buckets, the distributed
     * status watchdog, semaphore cleanup, and each active search. Before this
     * the client owned four platform threads at rest plus one per search.
     */
    final Scheduler scheduler;

    /** Chat rooms, split out of this class; see RoomRegistry. */
    private final RoomRegistry rooms;

    /** Applies option patches to a running client. */
    private final ClientReconfiguration reconfiguration = new ClientReconfiguration(this);

    /** User info, presence and browsing, split out; see UserDirectory. */
    private final UserDirectory users;

    /** Stateless server commands, split out; see ServerSession. */
    private final ServerSession server;

    /** Caller-facing search lifecycle, split out; see SearchCoordinator. */
    private final SearchCoordinator searchCoordinator;

    /** Transfer orchestration, split out; see TransferEngine. */
    private final TransferEngine transfers;

    volatile MessageConnection serverConnection;
    volatile Listener listener;
    volatile String address;
    volatile InetSocketAddress ipEndpoint;
    volatile String username;
    private volatile ServerInfo serverInfo = new ServerInfo();
    volatile SoulseekClientState state = SoulseekClientState.DISCONNECTED;
    private volatile Map<Integer, TransferInternal> downloads = new ConcurrentHashMap<>();
    private volatile Map<Integer, TransferInternal> uploads = new ConcurrentHashMap<>();
    private volatile Map<Integer, SearchInternal> searches = new ConcurrentHashMap<>();
    private final Map<String, Semaphore> uploadSemaphores = new ConcurrentHashMap<>();

    /** Creates a client with default options. */
    DefaultSoulseekClient(int minorVersion) {
        this(minorVersion, null);
    }

    /** Creates a client. */
    DefaultSoulseekClient(int minorVersion, SoulseekClientOptions options) {
        this(
                minorVersion,
                options,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    DefaultSoulseekClient(
            int minorVersion,
            SoulseekClientOptions options,
            MessageConnection serverConnection,
            ConnectionFactory connectionFactory,
            PeerConnectionManager peerConnectionManager,
            DistributedConnectionManager distributedConnectionManager,
            ServerMessageHandler serverMessageHandler,
            PeerMessageHandler peerMessageHandler,
            DistributedMessageHandler distributedMessageHandler,
            Listener listener,
            ListenerHandler listenerHandler,
            SearchResponder searchResponder,
            Waiter waiter,
            TokenFactory tokenFactory,
            DiagnosticSink diagnosticFactory,
            IOAdapter ioAdapter,
            TokenBucket uploadTokenBucket,
            TokenBucket downloadTokenBucket) {
        if (minorVersion <= 100) {
            throw new IllegalArgumentException("minorVersion must be greater than 100");
        }
        this.minorVersion = minorVersion;
        this.options = options == null ? new SoulseekClientOptions() : options;
        raiseEventsAsynchronously = this.options.isRaiseEventsAsynchronously();
        this.serverConnection = serverConnection;
        this.listener = listener;
        // Constructed before every component that schedules, since they all
        // share it.
        this.scheduler = new Scheduler("soulseek-client-timer");
        this.rooms = new RoomRegistry(this);
        this.users = new UserDirectory(this);
        this.server = new ServerSession(this);
        this.searchCoordinator = new SearchCoordinator(this);
        this.transfers = new TransferEngine(this);
        this.waiter = waiter == null ? new DefaultWaiter(this.options.getMessageTimeout(), scheduler) : waiter;
        this.tokenFactory = tokenFactory == null ? new TokenFactory(this.options.getStartingToken()) : tokenFactory;
        this.searchSemaphore = new Semaphore(this.options.getMaximumConcurrentSearches());
        this.globalDownloadSemaphore = new Semaphore(this.options.getMaximumConcurrentDownloads());
        this.globalUploadSemaphore = new Semaphore(this.options.getMaximumConcurrentUploads());
        this.ioAdapter = ioAdapter == null ? new IOAdapter() : ioAdapter;
        this.uploadTokenBucket = uploadTokenBucket == null
                ? new TokenBucket((this.options.getMaximumUploadSpeed() * 1024L) / 10, 100, scheduler)
                : uploadTokenBucket;
        this.downloadTokenBucket = downloadTokenBucket == null
                ? new TokenBucket((this.options.getMaximumDownloadSpeed() * 1024L) / 10, 100, scheduler)
                : downloadTokenBucket;
        this.connectionFactory = connectionFactory == null ? new DefaultConnectionFactory() : connectionFactory;

        diagnostic = diagnosticFactory == null
                ? new FilteringDiagnosticSink(
                        this.options.getMinimumDiagnosticLevel(),
                        eventData -> raise(Event.DIAGNOSTIC_GENERATED, eventData))
                : diagnosticFactory;
        GlobalDiagnostic.init(diagnostic);

        this.listenerHandler = listenerHandler == null ? new DefaultListenerHandler(this) : listenerHandler;
        this.searchResponder = searchResponder == null ? new DefaultSearchResponder(this) : searchResponder;
        this.peerMessageHandler = peerMessageHandler == null ? new DefaultPeerMessageHandler(this) : peerMessageHandler;
        this.distributedMessageHandler = distributedMessageHandler == null
                ? new DefaultDistributedMessageHandler(this)
                : distributedMessageHandler;
        this.peerConnectionManager =
                peerConnectionManager == null ? new DefaultPeerConnectionManager(this) : peerConnectionManager;
        this.distributedConnectionManager = distributedConnectionManager == null
                ? new DefaultDistributedConnectionManager(this, null, null, scheduler)
                : distributedConnectionManager;
        this.serverMessageHandler =
                serverMessageHandler == null ? new DefaultServerMessageHandler(this) : serverMessageHandler;

        bindEvents();

        scheduler.scheduleAtFixedRate(() -> users.cleanupUserEndpointSemaphoresAsync(), 5, 5, TimeUnit.MINUTES);
        scheduler.scheduleAtFixedRate(() -> cleanupUploadSemaphoresAsync(), 15, 15, TimeUnit.MINUTES);
    }

    /** Returns whether client events are configured as asynchronous. */
    public static boolean isRaiseEventsAsynchronously() {
        return raiseEventsAsynchronously;
    }

    /** Changes the process-wide source event-dispatch option. */
    public static void setRaiseEventsAsynchronously(boolean value) {
        raiseEventsAsynchronously = value;
    }

    /** Returns the connected server address text, or {@code null}. */
    public final String getAddress() {
        return address;
    }

    /** Returns distributed-network state as an immutable snapshot. */
    public final DistributedNetworkInfo getDistributedNetwork() {
        PeerEndpoint parent = distributedConnectionManager.getParent();
        List<DistributedPeer> children = distributedConnectionManager.getChildren() == null
                ? null
                : distributedConnectionManager.getChildren().stream()
                        .map(peer -> new DistributedPeer(peer.username(), peer.ipEndpoint()))
                        .toList();
        DistributedPeer parentSnapshot = parent == null
                ? new DistributedPeer("", null)
                : new DistributedPeer(parent.username(), parent.ipEndpoint());
        return new DistributedNetworkInfo(
                distributedConnectionManager.getAverageBroadcastLatency(),
                distributedConnectionManager.getBranchLevel(),
                distributedConnectionManager.getBranchRoot(),
                distributedConnectionManager.isBranchRoot(),
                distributedConnectionManager.getChildLimit(),
                distributedConnectionManager.canAcceptChildren(),
                children,
                parentSnapshot,
                distributedConnectionManager.hasParent());
    }

    /** Returns a snapshot of active downloads. */
    public final List<Transfer> getDownloads() {
        return downloads.values().stream().map(TransferInternal::toTransfer).toList();
    }

    /** Returns the connected server IP address, or {@code null}. */
    public final InetAddress getIpAddress() {
        return ipEndpoint == null ? null : ipEndpoint.getAddress();
    }

    /** Returns the connected server endpoint, or {@code null}. */
    public final InetSocketAddress getIpEndpoint() {
        return ipEndpoint;
    }

    /** Returns the configured client options. */
    @Override
    public final SoulseekClientOptions getOptions() {
        return options;
    }

    /** Returns the connected server port, or {@code null}. */
    public final Integer getPort() {
        return ipEndpoint == null ? null : ipEndpoint.getPort();
    }

    /** Returns the accumulated server information. */
    public final ServerInfo getServerInfo() {
        return serverInfo;
    }

    /** Returns current client state. */
    @Override
    public final SoulseekClientState getState() {
        return state;
    }

    /** Returns a snapshot of active uploads. */
    public final List<Transfer> getUploads() {
        return uploads.values().stream().map(TransferInternal::toTransfer).toList();
    }

    /** Returns the logged-in username, or {@code null}. */
    @Override
    public final String getUsername() {
        return username;
    }

    /** Returns the Soulseek network major version. */
    public final int getMajorVersion() {
        return MAJOR_VERSION;
    }

    /** Returns the caller-supplied client minor version. */
    public final int getMinorVersion() {
        return minorVersion;
    }

    /** Returns the next operation token. */
    @Override
    public int getNextToken() {
        return tokenFactory.nextToken();
    }

    /**
     * Connects to the default Soulseek server and logs in.
     *
     * @param requestedUsername the login username
     * @param password the login password
     * @return the connection operation
     */
    private CompletableFuture<Void> connectOperation(String requestedUsername, String password) {
        return connectOperation(DEFAULT_ADDRESS, DEFAULT_PORT, requestedUsername, password, CancellationSignal.none());
    }

    /**
     * Connects to the default Soulseek server and logs in.
     *
     * @param requestedUsername the login username
     * @param password the login password
     * @param cancellationSignal the cancellation signal
     * @return the connection operation
     */
    private CompletableFuture<Void> connectOperation(
            String requestedUsername, String password, CancellationSignal cancellationSignal) {
        return connectOperation(DEFAULT_ADDRESS, DEFAULT_PORT, requestedUsername, password, cancellationSignal);
    }

    /**
     * Connects to a Soulseek server and logs in.
     *
     * @param requestedAddress the server address
     * @param requestedPort the server port
     * @param requestedUsername the login username
     * @param password the login password
     * @return the connection operation
     */
    private CompletableFuture<Void> connectOperation(
            String requestedAddress, int requestedPort, String requestedUsername, String password) {
        return connectOperation(
                requestedAddress, requestedPort, requestedUsername, password, CancellationSignal.none());
    }

    /**
     * Connects to a Soulseek server and logs in.
     *
     * @param requestedAddress the server address
     * @param requestedPort the server port
     * @param requestedUsername the login username
     * @param password the login password
     * @param cancellationSignal the cancellation signal
     * @return the connection operation
     */
    private CompletableFuture<Void> connectOperation(
            String requestedAddress,
            int requestedPort,
            String requestedUsername,
            String password,
            CancellationSignal cancellationSignal) {
        requireText(requestedAddress, "address");
        if (requestedPort < 0 || requestedPort > 65_535) {
            throw new IllegalArgumentException("port must be between 0 and 65535 (specified: " + requestedPort + ")");
        }
        requireNonEmpty(requestedUsername, "username");
        requireNonEmpty(password, "password");
        if (state.contains(SoulseekClientState.CONNECTING) || state.contains(SoulseekClientState.LOGGING_IN)) {
            throw new IllegalStateException("A connection is already in the process of " + "being established");
        }
        if (state.contains(SoulseekClientState.CONNECTED)) {
            throw new IllegalStateException("The client is already connected");
        }

        InetAddress serverAddress;
        try {
            serverAddress = InetAddress.getByName(requestedAddress);
        } catch (UnknownHostException failure) {
            throw new AddressException(
                    "Failed to resolve address '" + requestedAddress + "': " + failureMessage(failure), failure);
        }

        if (options.isEnableListener()) {
            Listener probe = null;
            try {
                probe = clientListenerFactory.create(
                        options.getListenIpAddress(), options.getListenPort(), options.getIncomingConnectionOptions());
                probe.start();
            } catch (Throwable failure) {
                throw new ListenException("Failed to start listening on "
                        + options.getListenIpAddress() + ":"
                        + options.getListenPort()
                        + "; the IP and/or port may be in use or "
                        + "are otherwise unavailable");
            } finally {
                if (probe != null) {
                    probe.stop();
                }
            }
        }

        return connectInternalAsync(
                requestedAddress,
                new InetSocketAddress(serverAddress, requestedPort),
                requestedUsername,
                password,
                defaultToken(cancellationSignal));
    }

    /**
     * Applies a patch to the current client options.
     *
     * @param patch the option substitutions
     * @return whether reconnecting is required for full effect
     */
    private CompletableFuture<Boolean> reconfigureOptionsOperation(SoulseekClientOptionsPatch patch) {
        return reconfigureOptionsOperation(patch, CancellationSignal.none());
    }

    /**
     * Applies a patch to the current client options.
     *
     * @param patch the option substitutions
     * @param cancellationSignal the cancellation signal
     * @return whether reconnecting is required for full effect
     */
    private CompletableFuture<Boolean> reconfigureOptionsOperation(
            SoulseekClientOptionsPatch patch, CancellationSignal cancellationSignal) {
        Objects.requireNonNull(patch, "patch");
        boolean addressChanged = patch.getListenIpAddress() != null
                && !patch.getListenIpAddress().equals(options.getListenIpAddress());
        boolean portChanged = patch.getListenPort() != null && patch.getListenPort() != options.getListenPort();
        if (addressChanged || portChanged) {
            InetAddress newAddress =
                    patch.getListenIpAddress() == null ? options.getListenIpAddress() : patch.getListenIpAddress();
            int newPort = patch.getListenPort() == null ? options.getListenPort() : patch.getListenPort();
            Listener probe = null;
            try {
                probe = clientListenerFactory.create(newAddress, newPort, options.getIncomingConnectionOptions());
                probe.start();
            } catch (Throwable failure) {
                throw new ListenException("Failed to start listening on "
                        + newAddress + ":" + newPort
                        + "; the IP and/or port may be in use or "
                        + "are otherwise unavailable");
            } finally {
                if (probe != null) {
                    probe.stop();
                }
            }
        }
        return reconfiguration.reconfigureOptionsInternalAsync(patch, defaultToken(cancellationSignal));
    }

    /** Disconnects with the default reason. */
    public void disconnect() {
        disconnect(null, null);
    }

    /** Disconnects with a reason. */
    public void disconnect(String message) {
        disconnect(message, null);
    }

    /** Disconnects and records the causal exception. */
    public synchronized void disconnect(String message, Exception exception) {
        if (state.equals(SoulseekClientState.DISCONNECTED) || state.equals(SoulseekClientState.DISCONNECTING)) {
            return;
        }
        changeState(SoulseekClientState.DISCONNECTING, message, exception);
        String reason = message != null
                ? message
                : exception != null && exception.getMessage() != null ? exception.getMessage() : "Client disconnected";
        if (listener != null) {
            listener.stop();
        }
        if (serverConnection != null) {
            serverConnection.disconnect(reason, exception);
        }
        distributedConnectionManager.removeAndDisposeAll();
        distributedConnectionManager.resetStatus();
        for (SearchInternal search : new ArrayList<>(searches.values())) {
            search.cancel();
            search.close();
        }
        searches.clear();
        username = null;
        changeState(SoulseekClientState.DISCONNECTED, reason, exception);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        disconnect("Client is being disposed", new IllegalStateException("SoulseekClient is closed"));
        if (listener != null) {
            listener.stop();
        }
        peerConnectionManager.close();
        distributedConnectionManager.close();
        waiter.close();
        uploadTokenBucket.close();
        downloadTokenBucket.close();
        if (serverConnection != null) {
            serverConnection.close();
        }
        scheduler.close();
    }

    @Override
    public final Waiter getWaiter() {
        return waiter;
    }

    @Override
    public final Map<Integer, SearchInternal> getSearches() {
        return searches;
    }

    @Override
    public final Map<Integer, TransferInternal> getDownloadDictionary() {
        return downloads;
    }

    @Override
    public final PeerConnectionManager getPeerConnectionManager() {
        return peerConnectionManager;
    }

    @Override
    public final DistributedConnectionManager getDistributedConnectionManager() {
        return distributedConnectionManager;
    }

    @Override
    public final DistributedMessageHandler getDistributedMessageHandler() {
        return distributedMessageHandler;
    }

    @Override
    public final SearchResponder getSearchResponder() {
        return searchResponder;
    }

    @Override
    public final MessageConnection getServerConnection() {
        return serverConnection;
    }

    @Override
    public final PeerMessageHandler getPeerMessageHandler() {
        return peerMessageHandler;
    }

    @Override
    public final Listener getListener() {
        return listener;
    }

    final ServerMessageHandler getServerMessageHandler() {
        return serverMessageHandler;
    }

    final ListenerHandler getListenerHandler() {
        return listenerHandler;
    }

    final ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    @Override
    public IOAdapter getIoAdapter() {
        return ioAdapter;
    }

    @Override
    public TokenBucket getUploadTokenBucket() {
        return uploadTokenBucket;
    }

    @Override
    public TokenBucket getDownloadTokenBucket() {
        return downloadTokenBucket;
    }

    final Map<Integer, TransferInternal> getUploadsInternal() {
        return uploads;
    }

    /** Duplicate-transfer keys, owned by the transfer engine. */
    final Map<String, Boolean> getUniqueKeys() {
        return transfers.getUniqueKeys();
    }

    final Map<String, Semaphore> getUploadSemaphoresForTest() {
        return uploadSemaphores;
    }

    final Map<String, Semaphore> getUserEndpointSemaphoresForTest() {
        return users.getUserEndpointSemaphores();
    }

    final Semaphore getUploadSemaphoreSyncRootForTest() {
        return uploadSemaphoreSyncRoot;
    }

    void setStateForTest(SoulseekClientState value) {
        state = value;
    }

    void setServerConnectionForTest(MessageConnection value) {
        serverConnection = value;
    }

    void setListenerForTest(Listener value) {
        listener = value;
    }

    void setIpEndpointForTest(InetSocketAddress value) {
        ipEndpoint = value;
    }

    void setDownloadsForTest(Map<Integer, TransferInternal> value) {
        downloads = value;
    }

    void setUploadsForTest(Map<Integer, TransferInternal> value) {
        uploads = value;
    }

    void setSearchesForTest(Map<Integer, SearchInternal> value) {
        searches = value;
    }

    void setClientListenerFactoryForTest(ClientListenerFactory value) {
        clientListenerFactory = Objects.requireNonNull(value, "value");
    }

    void changeState(SoulseekClientState newState, String message, Exception exception) {
        SoulseekClientState previousState = state;
        state = newState;
        diagnostic.debug("Client state changed from " + previousState + " to "
                + newState
                + (message == null ? "" : "; message: " + message));
        raise(Event.STATE_CHANGED, new SoulseekClientStateChangedEvent(previousState, state, message, exception));
        if (state.equals(SoulseekClientState.CONNECTED)) {
            raise(Event.CONNECTED, null);
        } else if (state.equals(SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGED_IN))) {
            raise(Event.LOGGED_IN, null);
        } else if (state.equals(SoulseekClientState.DISCONNECTED)) {
            raise(Event.DISCONNECTED, new SoulseekClientDisconnectedEvent(message, exception));
        }
    }

    private void bindEvents() {
        listenerHandler.addDiagnosticGeneratedListener(
                (sender, eventData) -> raiseFrom(sender, Event.DIAGNOSTIC_GENERATED, eventData));
        searchResponder.addDiagnosticGeneratedListener(
                (sender, eventData) -> raiseFrom(sender, Event.DIAGNOSTIC_GENERATED, eventData));
        searchResponder.addRequestReceivedListener(
                (sender, eventData) -> raise(Event.SEARCH_REQUEST_RECEIVED, eventData));
        searchResponder.addResponseDeliveredListener(
                (sender, eventData) -> raise(Event.SEARCH_RESPONSE_DELIVERED, eventData));
        searchResponder.addResponseDeliveryFailedListener(
                (sender, eventData) -> raise(Event.SEARCH_RESPONSE_DELIVERY_FAILED, eventData));

        peerMessageHandler.addDiagnosticGeneratedListener(
                (sender, eventData) -> raiseFrom(sender, Event.DIAGNOSTIC_GENERATED, eventData));
        peerMessageHandler.addDownloadDeniedListener((sender, eventData) -> downloadDenied(eventData));
        peerMessageHandler.addDownloadFailedListener((sender, eventData) -> downloadFailed(eventData));
        distributedMessageHandler.addDiagnosticGeneratedListener(
                (sender, eventData) -> raiseFrom(sender, Event.DIAGNOSTIC_GENERATED, eventData));
        peerConnectionManager.addDiagnosticGeneratedListener(
                (sender, eventData) -> raiseFrom(sender, Event.DIAGNOSTIC_GENERATED, eventData));
        distributedConnectionManager.addDiagnosticGeneratedListener(
                (sender, eventData) -> raiseFrom(sender, Event.DIAGNOSTIC_GENERATED, eventData));
        distributedConnectionManager.addPromotedToBranchRootListener(
                (sender, eventData) -> raise(Event.PROMOTED_TO_DISTRIBUTED_BRANCH_ROOT, null));
        distributedConnectionManager.addDemotedFromBranchRootListener(
                (sender, eventData) -> raise(Event.DEMOTED_FROM_DISTRIBUTED_BRANCH_ROOT, null));
        distributedConnectionManager.addParentAdoptedListener(
                (sender, eventData) -> raise(Event.DISTRIBUTED_PARENT_ADOPTED, eventData));
        distributedConnectionManager.addParentDisconnectedListener(
                (sender, eventData) -> raise(Event.DISTRIBUTED_PARENT_DISCONNECTED, eventData));
        distributedConnectionManager.addChildAddedListener(
                (sender, eventData) -> raise(Event.DISTRIBUTED_CHILD_ADDED, eventData));
        distributedConnectionManager.addChildDisconnectedListener(
                (sender, eventData) -> raise(Event.DISTRIBUTED_CHILD_DISCONNECTED, eventData));
        distributedConnectionManager.addStateChangedListener(
                (sender, eventData) -> raise(Event.DISTRIBUTED_NETWORK_STATE_CHANGED, eventData));

        serverMessageHandler.addDiagnosticGeneratedListener(
                (sender, eventData) -> raiseFrom(sender, Event.DIAGNOSTIC_GENERATED, eventData));
        bindServerEvents();
    }

    private void bindServerEvents() {
        forwardServer(ServerMessageEvent.USER_CANNOT_CONNECT, Event.USER_CANNOT_CONNECT);
        forwardServer(ServerMessageEvent.USER_STATUS_CHANGED, Event.USER_STATUS_CHANGED);
        forwardServer(ServerMessageEvent.USER_STATISTICS_CHANGED, Event.USER_STATISTICS_CHANGED);
        forwardServer(ServerMessageEvent.PRIVATE_MESSAGE_RECEIVED, Event.PRIVATE_MESSAGE_RECEIVED);
        forwardServer(ServerMessageEvent.PRIVATE_ROOM_MEMBERSHIP_ADDED, Event.PRIVATE_ROOM_MEMBERSHIP_ADDED);
        forwardServer(ServerMessageEvent.PRIVATE_ROOM_MEMBERSHIP_REMOVED, Event.PRIVATE_ROOM_MEMBERSHIP_REMOVED);
        forwardServer(
                ServerMessageEvent.PRIVATE_ROOM_MODERATED_USER_LIST_RECEIVED,
                Event.PRIVATE_ROOM_MODERATED_USER_LIST_RECEIVED);
        forwardServer(ServerMessageEvent.PRIVATE_ROOM_MODERATION_ADDED, Event.PRIVATE_ROOM_MODERATION_ADDED);
        forwardServer(ServerMessageEvent.PRIVATE_ROOM_MODERATION_REMOVED, Event.PRIVATE_ROOM_MODERATION_REMOVED);
        forwardServer(ServerMessageEvent.PRIVATE_ROOM_USER_LIST_RECEIVED, Event.PRIVATE_ROOM_USER_LIST_RECEIVED);
        forwardServer(ServerMessageEvent.PRIVILEGED_USER_LIST_RECEIVED, Event.PRIVILEGED_USER_LIST_RECEIVED);
        forwardServer(ServerMessageEvent.PRIVILEGE_NOTIFICATION_RECEIVED, Event.PRIVILEGE_NOTIFICATION_RECEIVED);
        forwardServer(ServerMessageEvent.ROOM_MESSAGE_RECEIVED, Event.ROOM_MESSAGE_RECEIVED);
        forwardServer(ServerMessageEvent.ROOM_TICKER_LIST_RECEIVED, Event.ROOM_TICKER_LIST_RECEIVED);
        forwardServer(ServerMessageEvent.ROOM_TICKER_ADDED, Event.ROOM_TICKER_ADDED);
        forwardServer(ServerMessageEvent.ROOM_TICKER_REMOVED, Event.ROOM_TICKER_REMOVED);
        forwardServer(ServerMessageEvent.PUBLIC_CHAT_MESSAGE_RECEIVED, Event.PUBLIC_CHAT_MESSAGE_RECEIVED);
        forwardServer(ServerMessageEvent.ROOM_JOINED, Event.ROOM_JOINED);
        forwardServer(ServerMessageEvent.ROOM_LEFT, Event.ROOM_LEFT);
        forwardServer(ServerMessageEvent.ROOM_LIST_RECEIVED, Event.ROOM_LIST_RECEIVED);
        forwardServer(ServerMessageEvent.GLOBAL_MESSAGE_RECEIVED, Event.GLOBAL_MESSAGE_RECEIVED);
        forwardServer(ServerMessageEvent.DISTRIBUTED_NETWORK_RESET, Event.DISTRIBUTED_NETWORK_RESET);
        forwardServer(ServerMessageEvent.EXCLUDED_SEARCH_PHRASES_RECEIVED, Event.EXCLUDED_SEARCH_PHRASES_RECEIVED);
        serverMessageHandler.<ServerInfo>addListener(ServerMessageEvent.SERVER_INFO_RECEIVED, (sender, eventData) -> {
            serverInfo = serverInfo.with(
                    eventData.getParentMinSpeed(),
                    eventData.getParentSpeedRatio(),
                    eventData.getWishlistInterval(),
                    eventData.isSupporter());
            raise(Event.SERVER_INFO_RECEIVED, serverInfo);
        });
        serverMessageHandler.<Void>addListener(ServerMessageEvent.KICKED_FROM_SERVER, (sender, eventData) -> {
            diagnostic.info("Kicked from server.");
            raise(Event.KICKED_FROM_SERVER, null);
            disconnect("Kicked from server", new KickedFromServerException());
        });
    }

    private <T> void forwardServer(ServerMessageEvent source, Event target) {
        serverMessageHandler.<T>addListener(source, (sender, eventData) -> raise(target, eventData));
    }

    private void downloadDenied(DownloadDeniedEvent eventData) {
        try {
            List<TransferInternal> matching = downloads.values().stream()
                    .filter(download -> Objects.equals(download.getUsername(), eventData.getUsername())
                            && Objects.equals(download.getFilename(), eventData.getFilename()))
                    .toList();
            for (TransferInternal download : matching) {
                download.getRemoteTaskCompletionSource()
                        .completeExceptionally(new TransferRejectedException(eventData.getMessage()));
                diagnostic.debug("Download of " + download.getFilename() + " from "
                        + download.getUsername()
                        + " rejected by remote client (token: "
                        + download.getToken() + ")");
            }
        } catch (Throwable failure) {
            diagnostic.warning("Failed to mark download(s) rejected: " + failureMessage(failure), failure);
        } finally {
            raise(Event.DOWNLOAD_DENIED, eventData);
        }
    }

    private void downloadFailed(DownloadFailedEvent eventData) {
        try {
            List<TransferInternal> matching = downloads.values().stream()
                    .filter(download -> Objects.equals(download.getUsername(), eventData.getUsername())
                            && Objects.equals(download.getFilename(), eventData.getFilename()))
                    .toList();
            for (TransferInternal download : matching) {
                download.getRemoteTaskCompletionSource()
                        .completeExceptionally(
                                new TransferReportedFailedException("Download reported as failed by remote client"));
                diagnostic.debug("Download of " + download.getFilename() + " from "
                        + download.getUsername()
                        + " reported as failed by remote client (token: "
                        + download.getToken() + ")");
            }
        } catch (Throwable failure) {
            diagnostic.warning("Failed to mark download(s) failed: " + failureMessage(failure), failure);
        } finally {
            raise(Event.DOWNLOAD_FAILED, eventData);
        }
    }

    @Override
    public void requireLoggedIn(String operation) {
        if (!state.contains(SoulseekClientState.CONNECTED) || !state.contains(SoulseekClientState.LOGGED_IN)) {
            throw new IllegalStateException("The server connection must be connected and logged in to " + operation
                    + " (currently: " + state + ")");
        }
    }

    private CompletableFuture<Void> connectInternalAsync(
            String requestedAddress,
            InetSocketAddress requestedEndpoint,
            String requestedUsername,
            String password,
            CancellationSignal cancellationSignal) {
        CompletableFuture<Void> serialized = acquirePermit(stateSemaphore, cancellationSignal)
                .thenCompose(ignored -> {
                    CompletableFuture<Void> attempt;
                    if (state.contains(SoulseekClientState.CONNECTED)
                            && state.contains(SoulseekClientState.LOGGED_IN)) {
                        attempt = CompletableFuture.completedFuture(null);
                    } else {
                        attempt = performConnectAsync(
                                requestedAddress, requestedEndpoint, requestedUsername, password, cancellationSignal);
                    }
                    return attempt.whenComplete((result, failure) -> stateSemaphore.release());
                });

        return serialized.handle((result, failure) -> {
            if (failure == null) {
                return result;
            }
            Throwable cause = unwrap(failure);
            Throwable reported;
            if (cause instanceof LoginRejectedException
                    || cause instanceof CancellationException
                    || cause instanceof TimeoutException) {
                reported = cause;
            } else {
                reported = new SoulseekClientException("Failed to connect: " + failureMessage(cause), cause);
            }
            disconnect(failureMessage(reported), asException(reported));
            throw new CompletionException(reported);
        });
    }

    private CompletableFuture<Void> performConnectAsync(
            String requestedAddress,
            InetSocketAddress requestedEndpoint,
            String requestedUsername,
            String password,
            CancellationSignal cancellationSignal) {
        try {
            changeState(SoulseekClientState.CONNECTING, "Connecting", null);

            if (options.isEnableListener()) {
                listener = clientListenerFactory.create(
                        options.getListenIpAddress(), options.getListenPort(), options.getIncomingConnectionOptions());
                listener.addAcceptedListener(listenerHandler::handleConnection);
                listener.start();
            }

            serverConnection = connectionFactory.getServerConnection(
                    requestedEndpoint,
                    (sender, eventData) ->
                            changeState(SoulseekClientState.CONNECTED, "Connected to " + ipEndpoint, null),
                    (sender, eventData) -> disconnect(eventData.getMessage(), eventData.getException()),
                    serverMessageHandler::handleMessageRead,
                    serverMessageHandler::handleMessageWritten,
                    options.getServerConnectionOptions());

            CompletableFuture<Void> transportConnect;
            try {
                transportConnect = serverConnection.connectAsync(cancellationSignal);
            } catch (Throwable failure) {
                transportConnect = CompletableFuture.failedFuture(failure);
            }
            return transportConnect.thenCompose(ignored -> {
                address = requestedAddress;
                ipEndpoint = requestedEndpoint;
                changeState(SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGING_IN), "Logging in", null);
                return loginAsync(requestedUsername, password, cancellationSignal);
            });
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<Void> loginAsync(
            String requestedUsername, String password, CancellationSignal cancellationSignal) {
        CompletableFuture<LoginResponse> loginWait;
        try {
            loginWait = waiter.waitAsync(
                    new WaitKey(MessageCode.Server.LOGIN), LoginResponse.class, null, cancellationSignal);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }

        ByteArrayOutputStream loginMessages = new ByteArrayOutputStream();
        loginMessages.writeBytes(new LoginRequest(minorVersion, requestedUsername, password).toByteArray());
        loginMessages.writeBytes(new SetListenPortCommand(options.getListenPort()).toByteArray());

        return invokeServerByteWrite(loginMessages.toByteArray(), cancellationSignal)
                .thenCompose(ignored -> loginWait)
                .thenCompose(response -> {
                    if (!response.isSucceeded()) {
                        return CompletableFuture.failedFuture(new LoginRejectedException(
                                "The server rejected login attempt: " + response.getMessage()));
                    }
                    serverInfo = serverInfo.with(null, null, null, response.isSupporter());
                    raise(Event.SERVER_INFO_RECEIVED, serverInfo);
                    username = requestedUsername;
                    changeState(SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGED_IN), "Logged in", null);
                    return sendConfigurationMessagesAsync(cancellationSignal);
                });
    }

    CompletableFuture<Void> sendConfigurationMessagesAsync(CancellationSignal cancellationSignal) {
        return invokeServerWrite(new SetListenPortCommand(options.getListenPort()), cancellationSignal)
                .thenCompose(ignored -> invokeServerWrite(
                        new PrivateRoomToggle(options.isAcceptPrivateRoomInvitations()), cancellationSignal))
                .thenCompose(ignored -> {
                    try {
                        return distributedConnectionManager.updateStatusAsync(cancellationSignal);
                    } catch (Throwable failure) {
                        return CompletableFuture.failedFuture(failure);
                    }
                });
    }

    boolean isConnectedAndLoggedIn() {
        return state.contains(SoulseekClientState.CONNECTED) && state.contains(SoulseekClientState.LOGGED_IN);
    }

    private static Exception asException(Throwable failure) {
        if (failure instanceof Exception exception) {
            return exception;
        }
        return new RuntimeException(failure);
    }

    CompletableFuture<Void> cleanupUploadSemaphoresAsync() {
        if (!uploadSemaphoreSyncRoot.tryAcquire()) {
            return CompletableFuture.completedFuture(null);
        }
        try {
            for (Map.Entry<String, Semaphore> entry : uploadSemaphores.entrySet()) {
                Semaphore semaphore = entry.getValue();
                if (!semaphore.tryAcquire()) {
                    continue;
                }
                if (uploadSemaphores.remove(entry.getKey(), semaphore)) {
                    diagnostic.debug("Cleaned up upload semaphore for " + entry.getKey());
                } else {
                    semaphore.release();
                }
            }
            return CompletableFuture.completedFuture(null);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        } finally {
            uploadSemaphoreSyncRoot.release();
        }
    }

    /**
     * Waits for an internal operation and presents its failure the way a
     * blocking API should.
     *
     * <p>{@code join()} wraps everything in {@link CompletionException}, which
     * is an artifact of the async layer and has no business reaching a caller
     * of a blocking method. This unwraps it and rethrows the real cause.
     *
     * <p>A lapsed deadline arrives as the checked
     * {@link java.util.concurrent.TimeoutException}. Declaring that on every
     * operation that talks to the server would put a checked exception on most
     * of the public surface, which is the ceremony this API exists to remove;
     * the rest of the hierarchy is already unchecked. It is therefore mapped to
     * {@link NoResponseException}, which already means "an expected response
     * was not received" and is the semantically correct member of the existing
     * hierarchy. Recorded as D11 in docs/fork-divergence.md.
     */
    @Override
    <T> T unwrapped(CompletableFuture<T> operation) {
        try {
            return operation.join();
        } catch (Throwable failure) {
            Throwable cause = unwrap(failure);
            if (cause instanceof TimeoutException) {
                throw new NoResponseException(cause.getMessage(), cause);
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new SoulseekClientException(cause.getMessage(), cause);
        }
    }

    @Override
    public Transfer download(DownloadRequest request) {
        Objects.requireNonNull(request, "request");
        return unwrapped(
                !request.isToStream()
                        ? transfers.download(
                                request.getUsername(),
                                request.getRemoteFilename(),
                                request.getLocalFilename(),
                                request.getSize(),
                                request.getStartOffset(),
                                request.getToken(),
                                request.getOptions(),
                                request.getCancellationSignal())
                        : transfers.download(
                                request.getUsername(),
                                request.getRemoteFilename(),
                                request.getOutputStreamFactory(),
                                request.getSize(),
                                request.getStartOffset(),
                                request.getToken(),
                                request.getOptions(),
                                request.getCancellationSignal()));
    }

    @Override
    public TransferHandle enqueueDownload(DownloadRequest request) {
        Objects.requireNonNull(request, "request");
        return new TransferHandle(unwrapped(
                !request.isToStream()
                        ? transfers.enqueueDownload(
                                request.getUsername(),
                                request.getRemoteFilename(),
                                request.getLocalFilename(),
                                request.getSize(),
                                request.getStartOffset(),
                                request.getToken(),
                                request.getOptions(),
                                request.getCancellationSignal())
                        : transfers.enqueueDownload(
                                request.getUsername(),
                                request.getRemoteFilename(),
                                request.getOutputStreamFactory(),
                                request.getSize(),
                                request.getStartOffset(),
                                request.getToken(),
                                request.getOptions(),
                                request.getCancellationSignal())));
    }

    @Override
    public Transfer upload(UploadRequest request) {
        Objects.requireNonNull(request, "request");
        return unwrapped(
                !request.isFromStream()
                        ? transfers.upload(
                                request.getUsername(),
                                request.getRemoteFilename(),
                                request.getLocalFilename(),
                                request.getToken(),
                                request.getOptions(),
                                request.getCancellationSignal())
                        : transfers.upload(
                                request.getUsername(),
                                request.getRemoteFilename(),
                                request.getSize(),
                                request.getInputStreamFactory(),
                                request.getToken(),
                                request.getOptions(),
                                request.getCancellationSignal()));
    }

    @Override
    public TransferHandle enqueueUpload(UploadRequest request) {
        Objects.requireNonNull(request, "request");
        return new TransferHandle(unwrapped(
                !request.isFromStream()
                        ? transfers.enqueueUpload(
                                request.getUsername(),
                                request.getRemoteFilename(),
                                request.getLocalFilename(),
                                request.getToken(),
                                request.getOptions(),
                                request.getCancellationSignal())
                        : transfers.enqueueUpload(
                                request.getUsername(),
                                request.getRemoteFilename(),
                                request.getSize(),
                                request.getInputStreamFactory(),
                                request.getToken(),
                                request.getOptions(),
                                request.getCancellationSignal())));
    }

    @Override
    public SearchResult search(SearchRequest request) {
        Objects.requireNonNull(request, "request");
        return unwrapped(searchCoordinator.search(
                request.getQuery(),
                request.getScope(),
                request.getToken(),
                request.getOptions(),
                request.getCancellationSignal()));
    }

    @Override
    public Search search(SearchRequest request, Consumer<SearchResponse> responseHandler) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(responseHandler, "responseHandler");
        return unwrapped(searchCoordinator.search(
                request.getQuery(),
                responseHandler,
                request.getScope(),
                request.getToken(),
                request.getOptions(),
                request.getCancellationSignal()));
    }

    // ---- ClientContext, the seam the components delegate through ----------

    /** The periodic endpoint-semaphore sweep; exposed for tests. */
    CompletableFuture<Void> cleanupUserEndpointSemaphoresAsync() {
        return users.cleanupUserEndpointSemaphoresAsync();
    }

    @Override
    public CompletableFuture<java.net.InetSocketAddress> getUserEndpointOperation(
            String username, CancellationSignal cancellationSignal) {
        return users.getUserEndpoint(username, cancellationSignal);
    }

    @Override
    public CompletableFuture<java.net.InetSocketAddress> resolveUserEndpoint(
            String username, CancellationSignal cancellationSignal) {
        return users.getUserEndpoint(username, cancellationSignal);
    }

    @Override
    public CompletableFuture<Void> writeToPeer(
            MessageConnection connection, OutgoingMessage message, CancellationSignal cancellationSignal) {
        return invokeMessageWrite(connection, message, cancellationSignal);
    }

    @Override
    public void reportBrowseProgress(
            String requestedUsername,
            BrowseOptions operationOptions,
            long bytesTransferred,
            long size,
            AtomicBoolean completionEventFired) {
        BrowseProgressUpdatedEvent eventData =
                new BrowseProgressUpdatedEvent(requestedUsername, bytesTransferred, size);
        if (Double.compare(eventData.getPercentComplete(), 100.0) == 0) {
            completionEventFired.set(true);
        }
        if (operationOptions.getProgressUpdated() != null) {
            operationOptions
                    .getProgressUpdated()
                    .onProgressUpdated(new BrowseProgress(
                            eventData.getUsername(),
                            eventData.getBytesTransferred(),
                            eventData.getBytesRemaining(),
                            eventData.getPercentComplete(),
                            eventData.getSize()));
        }
        raise(Event.BROWSE_PROGRESS_UPDATED, eventData);
    }

    @Override
    public CompletableFuture<Void> acknowledgePrivateMessageOperation(
            int privateMessageId, CancellationSignal cancellationSignal) {
        return server.acknowledgePrivateMessage(privateMessageId, cancellationSignal);
    }

    @Override
    public CompletableFuture<Void> acknowledgePrivilegeNotificationOperation(
            int notificationId, CancellationSignal cancellationSignal) {
        return server.acknowledgePrivilegeNotification(notificationId, cancellationSignal);
    }

    @Override
    public java.util.Map<Integer, SearchInternal> getSearchRegistry() {
        return searches;
    }

    @Override
    public TokenFactory getTokenFactory() {
        return tokenFactory;
    }

    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public <T> void raiseSearchEvent(Event event, T eventData) {
        raise(event, eventData);
    }

    @Override
    public CompletableFuture<Void> writeBytesToServer(byte[] message, CancellationSignal cancellationSignal) {
        return invokeServerByteWrite(message, cancellationSignal);
    }

    @Override
    public java.util.Map<Integer, TransferInternal> getDownloadRegistry() {
        return downloads;
    }

    @Override
    public java.util.Map<Integer, TransferInternal> getUploadRegistry() {
        return uploads;
    }

    @Override
    public String getLoggedInUsername() {
        return username;
    }

    @Override
    public SoulseekClientOptions getClientOptions() {
        return options;
    }

    @Override
    public DiagnosticSink getDiagnostic() {
        return diagnostic;
    }

    @Override
    public CompletableFuture<Void> writeToServer(OutgoingMessage message, CancellationSignal cancellationSignal) {
        return invokeServerWrite(message, cancellationSignal);
    }

    @Override
    public CompletableFuture<Void> executeCorrelatedCommand(
            OutgoingMessage message, WaitKey waitKey, CancellationSignal cancellationSignal, String failurePrefix) {
        return executeCorrelatedServerCommand(message, waitKey, cancellationSignal, failurePrefix);
    }

    @Override
    @SafeVarargs
    public final <T> CompletableFuture<T> executeCorrelatedRequest(
            OutgoingMessage message,
            WaitKey waitKey,
            Class<T> resultType,
            CancellationSignal cancellationSignal,
            String failurePrefix,
            Class<? extends Throwable>... preservedFailures) {
        return executeCorrelatedServerRequest(
                message, waitKey, resultType, cancellationSignal, failurePrefix, preservedFailures);
    }

    private static <T> T await(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (Throwable failure) {
            throw new CompletionException(unwrap(failure));
        }
    }

    private CompletableFuture<Void> invokeServerByteWrite(byte[] message, CancellationSignal cancellationSignal) {
        try {
            return serverConnection.writeAsync(message, defaultToken(cancellationSignal));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<Void> writeServerAsync(
            OutgoingMessage message, CancellationSignal cancellationSignal, String failurePrefix) {
        return mapClientFailure(invokeServerWrite(message, cancellationSignal), failurePrefix);
    }

    private CompletableFuture<Void> executeCorrelatedServerCommand(
            OutgoingMessage message, WaitKey waitKey, CancellationSignal cancellationSignal, String failurePrefix) {
        CancellationSignal token = defaultToken(cancellationSignal);
        CompletableFuture<Void> wait;
        try {
            wait = waiter.waitAsync(waitKey, null, token);
        } catch (Throwable failure) {
            return mapClientFailure(CompletableFuture.failedFuture(failure), failurePrefix);
        }
        CompletableFuture<Void> responseWait = wait;
        CompletableFuture<Void> operation = invokeServerWrite(message, token).thenCompose(ignored -> responseWait);
        return mapClientFailure(operation, failurePrefix);
    }

    private <T> CompletableFuture<T> executeCorrelatedServerRequest(
            OutgoingMessage message,
            WaitKey waitKey,
            Class<T> resultType,
            CancellationSignal cancellationSignal,
            String failurePrefix,
            Class<? extends Throwable>... preservedFailures) {
        CancellationSignal token = defaultToken(cancellationSignal);
        CompletableFuture<T> wait;
        try {
            wait = waiter.waitAsync(waitKey, resultType, null, token);
        } catch (Throwable failure) {
            return mapClientFailure(CompletableFuture.failedFuture(failure), failurePrefix, preservedFailures);
        }
        CompletableFuture<T> operation = invokeServerWrite(message, token).thenCompose(ignored -> wait);
        return mapClientFailure(operation, failurePrefix, preservedFailures);
    }

    private CompletableFuture<Void> invokeServerWrite(OutgoingMessage message, CancellationSignal cancellationSignal) {
        return invokeMessageWrite(serverConnection, message, cancellationSignal);
    }

    private static CompletableFuture<Void> invokeMessageWrite(
            MessageConnection connection, OutgoingMessage message, CancellationSignal cancellationSignal) {
        CompletableFuture<Void> operation;
        try {
            operation = connection.writeAsync(
                    message, cancellationSignal == null ? CancellationSignal.none() : cancellationSignal);
        } catch (Throwable failure) {
            operation = CompletableFuture.failedFuture(failure);
        }
        return operation;
    }

    @Override
    public CancellationSignal defaultToken(CancellationSignal token) {
        return token == null ? CancellationSignal.none() : token;
    }

    @FunctionalInterface
    interface ClientListenerFactory {
        Listener create(InetAddress ipAddress, int port, ConnectionOptions connectionOptions);
    }

    @Override
    public void connect(String username, String password) {
        unwrapped(connectOperation(username, password));
    }

    @Override
    public void connect(String username, String password, CancellationSignal cancellationSignal) {
        unwrapped(connectOperation(username, password, cancellationSignal));
    }

    @Override
    public void connect(String address, int port, String username, String password) {
        unwrapped(connectOperation(address, port, username, password));
    }

    @Override
    public void connect(
            String address, int port, String username, String password, CancellationSignal cancellationSignal) {
        unwrapped(connectOperation(address, port, username, password, cancellationSignal));
    }

    @Override
    public List<Directory> getDirectoryContents(String username, String directoryName) {
        return unwrapped(users.getDirectoryContents(username, directoryName));
    }

    @Override
    public List<Directory> getDirectoryContents(String username, String directoryName, int token) {
        return unwrapped(users.getDirectoryContents(username, directoryName, token));
    }

    @Override
    public List<Directory> getDirectoryContents(
            String username, String directoryName, CancellationSignal cancellationSignal) {
        return unwrapped(users.getDirectoryContents(username, directoryName, cancellationSignal));
    }

    @Override
    public List<Directory> getDirectoryContents(
            String username, String directoryName, Integer token, CancellationSignal cancellationSignal) {
        return unwrapped(users.getDirectoryContents(username, directoryName, token, cancellationSignal));
    }

    @Override
    public InetSocketAddress getUserEndpoint(String username) {
        return unwrapped(users.getUserEndpoint(username));
    }

    @Override
    public InetSocketAddress getUserEndpoint(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users.getUserEndpoint(username, cancellationSignal));
    }

    @Override
    public Boolean reconfigureOptions(SoulseekClientOptionsPatch patch) {
        return unwrapped(reconfigureOptionsOperation(patch));
    }

    @Override
    public Boolean reconfigureOptions(SoulseekClientOptionsPatch patch, CancellationSignal cancellationSignal) {
        return unwrapped(reconfigureOptionsOperation(patch, cancellationSignal));
    }

    @Override
    RoomRegistry rooms() {
        return rooms;
    }

    @Override
    UserDirectory users() {
        return users;
    }

    @Override
    ServerSession server() {
        return server;
    }

    @Override
    SearchCoordinator searchCoordinator() {
        return searchCoordinator;
    }

    @Override
    TransferEngine transfers() {
        return transfers;
    }
}
