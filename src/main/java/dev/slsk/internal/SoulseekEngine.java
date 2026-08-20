// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.exceptions.AddressException;
import dev.slsk.exceptions.KickedFromServerException;
import dev.slsk.exceptions.ListenException;
import dev.slsk.exceptions.LoginRejectedException;
import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.common.CommonUtils;
import dev.slsk.internal.common.DefaultWaiter;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.IOAdapter;
import dev.slsk.internal.common.Locks;
import dev.slsk.internal.common.NetworkExecutor;
import dev.slsk.internal.common.Scheduler;
import dev.slsk.internal.common.TokenBucket;
import dev.slsk.internal.common.TokenFactory;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.concurrent.InterruptedOperationException;
import dev.slsk.internal.connection.ServerInfo;
import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.diagnostics.FilteringDiagnosticSink;
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
import dev.slsk.internal.messaging.handlers.PeerMessageHandler;
import dev.slsk.internal.messaging.handlers.ServerMessageEvent;
import dev.slsk.internal.messaging.handlers.ServerMessageHandler;
import dev.slsk.internal.messaging.messages.LoginRequest;
import dev.slsk.internal.messaging.messages.LoginResponse;
import dev.slsk.internal.messaging.messages.PrivateRoomToggle;
import dev.slsk.internal.messaging.messages.SetListenPortCommand;
import dev.slsk.internal.network.ConnectionFactory;
import dev.slsk.internal.network.DefaultConnectionFactory;
import dev.slsk.internal.network.DefaultListenerHandler;
import dev.slsk.internal.network.DistributedConnectionManager;
import dev.slsk.internal.network.DistributedNetwork;
import dev.slsk.internal.network.ListenerHandler;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.PeerConnectionManager;
import dev.slsk.internal.network.PeerNetwork;
import dev.slsk.internal.network.tcp.ConnectionMonitor;
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
import dev.slsk.internal.transfer.TransferInternal;
import dev.slsk.spi.ShareCatalog;
import dev.slsk.user.UserProfile;
import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * What runs underneath the facets.
 *
 * <p>This was {@code DefaultSoulseekClient}, the implementation of a public
 * interface with a hundred and eighty-five methods. Both are gone. What is left
 * is not a client — nothing outside this package can name it, let alone hold
 * one — but the machinery a client needs: the connection and login state
 * machine, the wiring of every component, and {@link EngineEvents} for the
 * facets to subscribe to.
 *
 * <p>It implemented nine collaborator interfaces once. Eight are gone: each
 * component takes what it uses, and what it used was almost entirely one-line
 * accessors. The ninth is {@link PeerServices} — what this client offers a
 * peer — and the state behind it is the upload, share and profile state that
 * leaves with {@code TransferEngine} in Phase 4.
 *
 * <p>What is left below the lifecycle is what the in-package collaborators
 * still read through the engine. It is package-private now that no interface
 * declares it, and it shrinks as each of them takes its own ports.
 */
final class SoulseekEngine implements AutoCloseable {

    private static final int MAJOR_VERSION = 170;
    private static final String DEFAULT_ADDRESS = "server.slsknet.org";
    private static final int DEFAULT_PORT = 2271;

    volatile SoulseekClientOptions options;
    private final int minorVersion;
    final Waiter waiter;
    private final TokenFactory tokenFactory;
    private final ReentrantLock stateLock = new ReentrantLock();
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
    /**
     * What the engine tells the facets. Constructed before the diagnostic sink,
     * because the sink raises through it.
     */
    private final EngineEvents events = new EngineEvents(this::reportListenerFault);
    /**
     * Sweeps every connection this client opens, on this client's scheduler.
     *
     * <p>It was a static field on {@code SocketConnection} over a static
     * two-thread platform pool, shared by every client in the JVM and shut down
     * by none of them. Closing a client now takes its sweep with it.
     */
    final ConnectionMonitor connectionMonitor;

    volatile ClientListenerFactory clientListenerFactory;
    private volatile ShareCatalog catalog = ShareCatalog.empty();
    private volatile UserProfile profile = UserProfile.empty();
    /**
     * Who the server said has bought privileges.
     *
     * <p>Sent once on login as a list, and the only way to know: there is no
     * per-user query for it. Kept because privileged users jump the upload
     * queue, which is protocol-mandated rather than a matter of taste.
     */
    private volatile java.util.Set<String> privilegedUsers = java.util.Set.of();

    private final AtomicBoolean closed = new AtomicBoolean();
    private final NetworkExecutor networkExecutor;
    /**
     * The client's single timer thread. Every component that needs delayed or
     * periodic work shares it: the waiter, both token buckets, the distributed
     * status watchdog, semaphore cleanup, and each active search. Before this
     * the client owned four platform threads at rest plus one per search.
     */
    final Scheduler scheduler;

    /** Chat rooms, split out of this class; see RoomRegistry. */
    private final RoomRegistry rooms;

    /** User info, presence and browsing, split out; see UserDirectory. */
    private final UserDirectory users;

    /** The server connection and everything said over it; see ServerLink. */
    private final ServerLink server;

    /** Searches and the registry of the ones in flight; see SearchDomain. */
    private final SearchDomain searchDomain;

    /** Transfers, the two registries, the limits and what a peer may ask of us. */
    private final TransferDomain transfers;

    volatile Listener listener;
    volatile String address;
    volatile InetSocketAddress ipEndpoint;
    private volatile ServerInfo serverInfo = new ServerInfo();
    volatile SoulseekClientState state = SoulseekClientState.DISCONNECTED;

    /** Serializes {@link #changeState}; see its javadoc. */
    private final Object stateChangeLock = new Object();

    /** Creates a client with default options. */
    SoulseekEngine(int minorVersion) {
        this(minorVersion, null);
    }

    /** Creates a client. */
    SoulseekEngine(int minorVersion, SoulseekClientOptions options) {
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

    SoulseekEngine(
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
        this.listener = listener;
        // Constructed before every component that schedules, since they all
        // share it.
        this.networkExecutor = new NetworkExecutor();
        this.scheduler = new Scheduler("soulseek-client-timer", networkExecutor.executor());
        this.waiter = waiter == null ? new DefaultWaiter(this.options.messageTimeout(), scheduler) : waiter;
        // Before every component that writes to the server, because they are
        // built with it rather than reaching back through the engine for it.
        diagnostic = diagnosticFactory == null
                ? new FilteringDiagnosticSink(
                        this.options.minimumDiagnosticLevel(),
                        eventData -> events.publish(Kind.DIAGNOSTIC_GENERATED, eventData))
                : diagnosticFactory.forSource(SoulseekEngine.class);
        this.server = new ServerLink(this.waiter, diagnostic, () -> state);
        this.server.connection(serverConnection);
        this.rooms = new RoomRegistry(this.waiter, server);
        this.users = new UserDirectory(this, server);
        this.searchDomain = new SearchDomain(this, server);
        this.tokenFactory = tokenFactory == null ? new TokenFactory(this.options.startingToken()) : tokenFactory;
        this.ioAdapter = ioAdapter == null ? new IOAdapter() : ioAdapter;
        this.uploadTokenBucket = uploadTokenBucket == null
                ? new TokenBucket((this.options.maximumUploadSpeed() * 1024L) / 10, Duration.ofMillis(100), scheduler)
                : uploadTokenBucket;
        this.downloadTokenBucket = downloadTokenBucket == null
                ? new TokenBucket((this.options.maximumDownloadSpeed() * 1024L) / 10, Duration.ofMillis(100), scheduler)
                : downloadTokenBucket;
        // After the buckets and the token factory it takes, and before the peer
        // message handler, which answers a peer through it. The connection
        // manager is a supplier because it is built after this.
        this.transfers = new TransferDomain(
                this::getOptions,
                diagnostic,
                this.waiter,
                this::getPeerConnectionManager,
                users::getUserEndpoint,
                server,
                this.tokenFactory,
                this.ioAdapter,
                this.downloadTokenBucket,
                this.uploadTokenBucket,
                this::catalog,
                this::profile,
                this::isPrivileged,
                scheduler,
                networkExecutor);
        this.connectionMonitor = new ConnectionMonitor(scheduler);
        this.clientListenerFactory = (address, port, connectionOptions) ->
                new SocketListener(address, port, connectionOptions, connectionMonitor, networkExecutor.executor());
        this.connectionFactory = connectionFactory == null
                ? new DefaultConnectionFactory(connectionMonitor, networkExecutor.executor())
                : connectionFactory;

        this.listenerHandler = listenerHandler == null
                ? new DefaultListenerHandler(
                        this::getOptions,
                        () -> this.listener,
                        this::getPeerConnectionManager,
                        this::getDistributedConnectionManager,
                        this.waiter,
                        this::getSearchResponder)
                : listenerHandler;
        this.searchResponder = searchResponder == null
                ? new DefaultSearchResponder(
                        this::getOptions,
                        this::getPeerConnectionManager,
                        this.tokenFactory,
                        users::getUserEndpoint,
                        this::catalog,
                        server::username,
                        transfers::advertisedUploadSpeed)
                : searchResponder;
        this.peerMessageHandler = peerMessageHandler == null
                ? new DefaultPeerMessageHandler(
                        this::getOptions,
                        this.waiter,
                        searchDomain::registry,
                        this::getDownloadRegistry,
                        server::username,
                        this.transfers,
                        null,
                        networkExecutor.executor())
                : peerMessageHandler;
        this.distributedMessageHandler = distributedMessageHandler == null
                ? new DefaultDistributedMessageHandler(
                        this::getOptions,
                        server,
                        this.tokenFactory,
                        this.waiter,
                        this::getDistributedConnectionManager,
                        this::getSearchResponder,
                        null,
                        networkExecutor)
                : distributedMessageHandler;
        this.peerConnectionManager = peerConnectionManager == null
                ? new PeerNetwork(
                        this::getOptions,
                        server,
                        this.waiter,
                        this.tokenFactory,
                        this.peerMessageHandler,
                        this.connectionFactory,
                        null,
                        scheduler)
                : peerConnectionManager;
        this.distributedConnectionManager = distributedConnectionManager == null
                ? new DistributedNetwork(
                        this::getOptions,
                        server,
                        this.waiter,
                        this.tokenFactory,
                        this::getDistributedMessageHandler,
                        this.connectionFactory,
                        null,
                        scheduler)
                : distributedConnectionManager;
        this.serverMessageHandler = serverMessageHandler == null
                ? new DefaultServerMessageHandler(
                        this::getOptions,
                        server,
                        this.waiter,
                        searchDomain::registry,
                        this::getDownloadRegistry,
                        this::getPeerConnectionManager,
                        this::getDistributedConnectionManager,
                        this::getDistributedMessageHandler,
                        this::getSearchResponder,
                        null,
                        networkExecutor)
                : serverMessageHandler;

        bindEvents();

        scheduler.scheduleAtFixedRate(users::cleanupUserEndpointSemaphores, 5, 5, TimeUnit.MINUTES);
        scheduler.scheduleAtFixedRate(transfers::cleanupUploadSemaphores, 15, 15, TimeUnit.MINUTES);
    }

    /**
     * Where a contained listener fault goes.
     *
     * <p>These events are raised on read loops. Before containment a facet that
     * threw while translating one took the connection down with it.
     */
    private void reportListenerFault(EngineEvents.Kind kind, Throwable failure) {
        if (diagnostic != null) {
            diagnostic.warning(
                    "A listener for " + kind + " threw; the event was still delivered " + "to the rest", failure);
        }
    }

    /** The channel the facets subscribe to. */
    EngineEvents events() {
        return events;
    }

    /**
     * What peers are served from.
     *
     * <p>Volatile and replaceable, because {@code Shares.rescan} swaps in a new
     * one and {@code Shares.catalog} replaces it outright, both while peers are
     * connected. A browse in flight finishes against the catalog it started
     * with, which is the only thing a snapshot-shaped read can promise.
     */
    ShareCatalog catalog() {
        return catalog;
    }

    /**
     * Installs the catalog peers are served from.
     *
     * @param value the catalog, or {@code null} for the empty one
     */
    void setShareCatalog(ShareCatalog value) {
        catalog = value == null ? ShareCatalog.empty() : value;
    }

    /**
     * Returns what peers are told about this account.
     *
     * @return the profile, never {@code null}
     */
    UserProfile profile() {
        return profile;
    }

    /**
     * Applies a download rate ceiling.
     *
     * <p>The bucket refills ten times a second, so its capacity is a tenth of
     * the rate. Unlimited is expressed as a capacity nothing will reach rather
     * than as a special case, because a branch on "is this unlimited" is a
     * branch that gets forgotten on one of the two paths.
     *
     * @param limit the ceiling
     */
    void setDownloadSpeedLimit(dev.slsk.transfer.Bandwidth limit) {
        downloadTokenBucket.setCapacity(
                limit == null || limit.isUnlimited() ? Long.MAX_VALUE / 16 : Math.max(1, limit.bytesPerSecond() / 10));
    }

    /**
     * Returns whether the server said a user has bought privileges.
     *
     * @param username who
     * @return whether they are privileged
     */
    boolean isPrivileged(String username) {
        return username != null && privilegedUsers.contains(username);
    }

    /**
     * Sets what peers are told about this account.
     *
     * @param value the profile, or {@code null} for the empty one
     */
    void setProfile(UserProfile value) {
        profile = value == null ? UserProfile.empty() : value;
    }

    /** Returns the connected server address text, or {@code null}. */
    public final String getAddress() {
        return address;
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
    public final SoulseekClientState getState() {
        return state;
    }

    /** Returns the logged-in username, or {@code null}. */
    public final String getUsername() {
        return server.username();
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
    private void connectOperation(String requestedUsername, String password) {
        connectOperation(DEFAULT_ADDRESS, DEFAULT_PORT, requestedUsername, password, CancellationSignal.none());
    }

    /**
     * Connects to the default Soulseek server and logs in.
     *
     * @param requestedUsername the login username
     * @param password the login password
     * @param cancellationSignal the cancellation signal
     * @return the connection operation
     */
    private void connectOperation(String requestedUsername, String password, CancellationSignal cancellationSignal) {
        connectOperation(DEFAULT_ADDRESS, DEFAULT_PORT, requestedUsername, password, cancellationSignal);
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
    private void connectOperation(
            String requestedAddress, int requestedPort, String requestedUsername, String password) {
        connectOperation(requestedAddress, requestedPort, requestedUsername, password, CancellationSignal.none());
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
    private void connectOperation(
            String requestedAddress,
            int requestedPort,
            String requestedUsername,
            String password,
            CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedAddress, "address");
        if (requestedPort < 0 || requestedPort > 65_535) {
            throw new IllegalArgumentException("port must be between 0 and 65535: " + requestedPort);
        }
        CommonUtils.requireNonEmpty(requestedUsername, "username");
        CommonUtils.requireNonEmpty(password, "password");
        if (state == SoulseekClientState.CONNECTING || state == SoulseekClientState.LOGGING_IN) {
            throw new IllegalStateException("A connection is already in the process of " + "being established");
        }
        if (state.isConnected()) {
            throw new IllegalStateException("The client is already connected");
        }

        InetAddress serverAddress;
        try {
            serverAddress = InetAddress.getByName(requestedAddress);
        } catch (UnknownHostException failure) {
            throw new AddressException(
                    "Failed to resolve address '" + requestedAddress + "': " + Failures.message(failure), failure);
        }

        if (options.enableListener()) {
            Listener probe = null;
            try {
                probe = clientListenerFactory.create(
                        options.listenIpAddress(), options.listenPort(), options.incomingConnectionOptions());
                probe.start();
            } catch (Throwable failure) {
                throw new ListenException("Failed to start listening on "
                        + options.listenIpAddress() + ":"
                        + options.listenPort()
                        + "; the IP and/or port may be in use or "
                        + "are otherwise unavailable");
            } finally {
                if (probe != null) {
                    probe.stop();
                }
            }
        }

        connectInternal(
                requestedAddress,
                new InetSocketAddress(serverAddress, requestedPort),
                requestedUsername,
                password,
                CommonUtils.token(cancellationSignal));
    }

    /**
     * Applies a patch to the current client options.
     *
     * @param patch the option substitutions
     * @return whether reconnecting is required for full effect
     */
    private boolean reconfigureOptionsOperation(SoulseekClientOptionsPatch patch) {
        return reconfigureOptionsOperation(patch, CancellationSignal.none());
    }

    /**
     * Applies a patch to the current client options.
     *
     * @param patch the option substitutions
     * @param cancellationSignal the cancellation signal
     * @return whether reconnecting is required for full effect
     */
    private boolean reconfigureOptionsOperation(
            SoulseekClientOptionsPatch patch, CancellationSignal cancellationSignal) {
        Objects.requireNonNull(patch, "patch");
        boolean addressChanged = patch.listenIpAddress()
                .filter(value -> !value.equals(options.listenIpAddress()))
                .isPresent();
        boolean portChanged = patch.listenPort()
                .filter(value -> value != options.listenPort())
                .isPresent();
        if (addressChanged || portChanged) {
            InetAddress newAddress = patch.listenIpAddress().orElse(options.listenIpAddress());
            int newPort = patch.listenPort().orElse(options.listenPort());
            Listener probe = null;
            try {
                probe = clientListenerFactory.create(newAddress, newPort, options.incomingConnectionOptions());
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
        return reconfigureOptionsInternal(patch, CommonUtils.token(cancellationSignal));
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
        MessageConnection connection = server.connection();
        if (connection != null) {
            connection.disconnect(reason, exception);
        }
        distributedConnectionManager.removeAndCloseAll();
        distributedConnectionManager.resetStatus();
        searchDomain.cancelAll();
        server.username(null);
        changeState(SoulseekClientState.DISCONNECTED, reason, exception);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        disconnect("Client is being closed", new IllegalStateException("The client is closed"));
        if (listener != null) {
            listener.stop();
        }
        peerConnectionManager.close();
        distributedConnectionManager.close();
        waiter.close();
        uploadTokenBucket.close();
        downloadTokenBucket.close();
        MessageConnection connection = server.connection();
        if (connection != null) {
            connection.close();
        }
        connectionMonitor.close();
        scheduler.close();
        networkExecutor.close();
    }

    final Waiter getWaiter() {
        return waiter;
    }

    final Map<Integer, SearchInternal> getSearches() {
        return searchDomain.registry();
    }

    final PeerConnectionManager getPeerConnectionManager() {
        return peerConnectionManager;
    }

    final DistributedConnectionManager getDistributedConnectionManager() {
        return distributedConnectionManager;
    }

    final DistributedMessageHandler getDistributedMessageHandler() {
        return distributedMessageHandler;
    }

    final SearchResponder getSearchResponder() {
        return searchResponder;
    }

    /** Duplicate-transfer keys, owned by the transfer domain. */
    final Set<String> getUniqueKeys() {
        return transfers.uniqueKeys();
    }

    final Map<String, Semaphore> getUserEndpointSemaphoresForTest() {
        return users.getUserEndpointSemaphores();
    }

    void setStateForTest(SoulseekClientState value) {
        state = value;
    }

    void setServerConnectionForTest(MessageConnection value) {
        server.connection(value);
    }

    void setListenerForTest(Listener value) {
        listener = value;
    }

    void setIpEndpointForTest(InetSocketAddress value) {
        ipEndpoint = value;
    }

    void setDownloadsForTest(Map<Integer, TransferInternal> value) {
        transfers.downloadsForTest(value);
    }

    void setUploadsForTest(Map<Integer, TransferInternal> value) {
        transfers.uploadsForTest(value);
    }

    void setSearchesForTest(Map<Integer, SearchInternal> value) {
        searchDomain.registry(value);
    }

    void setClientListenerFactoryForTest(ClientListenerFactory value) {
        clientListenerFactory = Objects.requireNonNull(value, "value");
    }

    /**
     * One transition at a time. The callers are on different threads and
     * different monitors — the connect thread under its semaphore, the
     * connection factory's connected callback, disconnect under the engine
     * monitor — and an unsynchronized read-modify-write let a server drop
     * during login interleave two transitions: stale previousState on the
     * events, and LOGGED_IN observable after DISCONNECTED. The raise happens
     * under the same lock so the events leave in the order the transitions
     * happened; delivery is queued per facet bus, so nothing slow runs here.
     */
    void changeState(SoulseekClientState newState, String message, Exception exception) {
        synchronized (stateChangeLock) {
            SoulseekClientState previousState = state;
            state = newState;
            diagnostic.debug("Client state changed from " + previousState + " to "
                    + newState
                    + (message == null ? "" : "; message: " + message));
            events.publish(
                    Kind.STATE_CHANGED, new SoulseekClientStateChangedEvent(previousState, state, message, exception));
            if (state.equals(SoulseekClientState.CONNECTED)) {
                events.publish(Kind.CONNECTED, null);
            } else if (state == SoulseekClientState.LOGGED_IN) {
                events.publish(Kind.LOGGED_IN, null);
            } else if (state.equals(SoulseekClientState.DISCONNECTED)) {
                events.publish(Kind.DISCONNECTED, new SoulseekClientDisconnectedEvent(message, exception));
            }
        }
    }

    private void bindEvents() {
        listenerHandler.subscribe(eventData -> events.publish(Kind.DIAGNOSTIC_GENERATED, eventData));
        searchResponder.subscribe(eventData -> events.publish(Kind.DIAGNOSTIC_GENERATED, eventData));
        searchResponder.subscribe(
                SearchResponder.Kind.REQUEST_RECEIVED,
                eventData -> events.publish(Kind.SEARCH_REQUEST_RECEIVED, eventData));
        searchResponder.subscribe(
                SearchResponder.Kind.RESPONSE_DELIVERED,
                eventData -> events.publish(Kind.SEARCH_RESPONSE_DELIVERED, eventData));
        searchResponder.subscribe(
                SearchResponder.Kind.RESPONSE_DELIVERY_FAILED,
                eventData -> events.publish(Kind.SEARCH_RESPONSE_DELIVERY_FAILED, eventData));

        peerMessageHandler.subscribe(eventData -> events.publish(Kind.DIAGNOSTIC_GENERATED, eventData));
        peerMessageHandler.subscribe(PeerMessageHandler.Kind.DOWNLOAD_DENIED, this::downloadDenied);
        peerMessageHandler.subscribe(PeerMessageHandler.Kind.DOWNLOAD_FAILED, this::downloadFailed);
        distributedMessageHandler.subscribe(eventData -> events.publish(Kind.DIAGNOSTIC_GENERATED, eventData));
        peerConnectionManager.subscribe(eventData -> events.publish(Kind.DIAGNOSTIC_GENERATED, eventData));
        distributedConnectionManager.subscribe(eventData -> events.publish(Kind.DIAGNOSTIC_GENERATED, eventData));
        distributedConnectionManager.subscribe(
                DistributedConnectionManager.Kind.PROMOTED_TO_BRANCH_ROOT,
                eventData -> events.publish(Kind.PROMOTED_TO_DISTRIBUTED_BRANCH_ROOT, null));
        distributedConnectionManager.subscribe(
                DistributedConnectionManager.Kind.DEMOTED_FROM_BRANCH_ROOT,
                eventData -> events.publish(Kind.DEMOTED_FROM_DISTRIBUTED_BRANCH_ROOT, null));
        distributedConnectionManager.subscribe(
                DistributedConnectionManager.Kind.PARENT_ADOPTED,
                eventData -> events.publish(Kind.DISTRIBUTED_PARENT_ADOPTED, eventData));
        distributedConnectionManager.subscribe(
                DistributedConnectionManager.Kind.PARENT_DISCONNECTED,
                eventData -> events.publish(Kind.DISTRIBUTED_PARENT_DISCONNECTED, eventData));
        distributedConnectionManager.subscribe(
                DistributedConnectionManager.Kind.CHILD_ADDED,
                eventData -> events.publish(Kind.DISTRIBUTED_CHILD_ADDED, eventData));
        distributedConnectionManager.subscribe(
                DistributedConnectionManager.Kind.CHILD_DISCONNECTED,
                eventData -> events.publish(Kind.DISTRIBUTED_CHILD_DISCONNECTED, eventData));
        distributedConnectionManager.subscribe(
                DistributedConnectionManager.Kind.STATE_CHANGED,
                eventData -> events.publish(Kind.DISTRIBUTED_NETWORK_STATE_CHANGED, eventData));

        serverMessageHandler.subscribe(eventData -> events.publish(Kind.DIAGNOSTIC_GENERATED, eventData));
        bindServerEvents();
    }

    private void bindServerEvents() {
        forwardServer(ServerMessageEvent.USER_CANNOT_CONNECT, Kind.USER_CANNOT_CONNECT);
        forwardServer(ServerMessageEvent.USER_STATUS_CHANGED, Kind.USER_STATUS_CHANGED);
        serverMessageHandler.<dev.slsk.internal.user.UserStatistics>subscribe(
                ServerMessageEvent.USER_STATISTICS_CHANGED, statistics -> {
                    // A statistics response naming us carries the upload
                    // average the server computed from our reports, which is
                    // what search responses advertise; the transfer domain
                    // adopts it before the event goes out.
                    if (statistics != null && statistics.username().equals(server.username())) {
                        transfers.advertisedUploadSpeed(statistics.averageSpeed());
                    }
                    events.publish(Kind.USER_STATISTICS_CHANGED, statistics);
                });
        forwardServer(ServerMessageEvent.PRIVATE_MESSAGE_RECEIVED, Kind.PRIVATE_MESSAGE_RECEIVED);
        forwardServer(ServerMessageEvent.PRIVATE_ROOM_MEMBERSHIP_ADDED, Kind.PRIVATE_ROOM_MEMBERSHIP_ADDED);
        forwardServer(ServerMessageEvent.PRIVATE_ROOM_MEMBERSHIP_REMOVED, Kind.PRIVATE_ROOM_MEMBERSHIP_REMOVED);
        forwardServer(
                ServerMessageEvent.PRIVATE_ROOM_MODERATED_USER_LIST_RECEIVED,
                Kind.PRIVATE_ROOM_MODERATED_USER_LIST_RECEIVED);
        forwardServer(ServerMessageEvent.PRIVATE_ROOM_MODERATION_ADDED, Kind.PRIVATE_ROOM_MODERATION_ADDED);
        forwardServer(ServerMessageEvent.PRIVATE_ROOM_MODERATION_REMOVED, Kind.PRIVATE_ROOM_MODERATION_REMOVED);
        forwardServer(ServerMessageEvent.PRIVATE_ROOM_USER_LIST_RECEIVED, Kind.PRIVATE_ROOM_USER_LIST_RECEIVED);
        serverMessageHandler.<java.util.List<String>>subscribe(
                ServerMessageEvent.PRIVILEGED_USER_LIST_RECEIVED, eventData -> {
                    privilegedUsers = eventData == null ? java.util.Set.of() : java.util.Set.copyOf(eventData);
                    events.publish(Kind.PRIVILEGED_USER_LIST_RECEIVED, eventData);
                });
        forwardServer(ServerMessageEvent.PRIVILEGE_NOTIFICATION_RECEIVED, Kind.PRIVILEGE_NOTIFICATION_RECEIVED);
        forwardServer(ServerMessageEvent.ROOM_MESSAGE_RECEIVED, Kind.ROOM_MESSAGE_RECEIVED);
        forwardServer(ServerMessageEvent.ROOM_TICKER_LIST_RECEIVED, Kind.ROOM_TICKER_LIST_RECEIVED);
        forwardServer(ServerMessageEvent.ROOM_TICKER_ADDED, Kind.ROOM_TICKER_ADDED);
        forwardServer(ServerMessageEvent.ROOM_TICKER_REMOVED, Kind.ROOM_TICKER_REMOVED);
        forwardServer(ServerMessageEvent.PUBLIC_CHAT_MESSAGE_RECEIVED, Kind.PUBLIC_CHAT_MESSAGE_RECEIVED);
        forwardServer(ServerMessageEvent.ROOM_JOINED, Kind.ROOM_JOINED);
        forwardServer(ServerMessageEvent.ROOM_LEFT, Kind.ROOM_LEFT);
        forwardServer(ServerMessageEvent.ROOM_LIST_RECEIVED, Kind.ROOM_LIST_RECEIVED);
        forwardServer(ServerMessageEvent.GLOBAL_MESSAGE_RECEIVED, Kind.GLOBAL_MESSAGE_RECEIVED);
        forwardServer(ServerMessageEvent.DISTRIBUTED_NETWORK_RESET, Kind.DISTRIBUTED_NETWORK_RESET);
        forwardServer(ServerMessageEvent.EXCLUDED_SEARCH_PHRASES_RECEIVED, Kind.EXCLUDED_SEARCH_PHRASES_RECEIVED);
        serverMessageHandler.<ServerInfo>subscribe(ServerMessageEvent.SERVER_INFO_RECEIVED, eventData -> {
            serverInfo = serverInfo.with(
                    eventData.parentMinSpeed(),
                    eventData.parentSpeedRatio(),
                    eventData.wishlistInterval(),
                    eventData.supporter());
            events.publish(Kind.SERVER_INFO_RECEIVED, serverInfo);
        });
        serverMessageHandler.<Void>subscribe(ServerMessageEvent.KICKED_FROM_SERVER, eventData -> {
            diagnostic.info("Kicked from server.");
            events.publish(Kind.KICKED_FROM_SERVER, null);
            disconnect("Kicked from server", new KickedFromServerException());
        });
    }

    private <T> void forwardServer(ServerMessageEvent source, EngineEvents.Kind target) {
        serverMessageHandler.<T>subscribe(source, eventData -> events.publish(target, eventData));
    }

    private void downloadDenied(DownloadDeniedEvent eventData) {
        try {
            transfers.deniedByPeer(eventData.username(), eventData.filename(), eventData.message());
        } catch (Throwable failure) {
            diagnostic.warning("Failed to mark download(s) rejected: " + Failures.message(failure), failure);
        } finally {
            events.publish(Kind.DOWNLOAD_DENIED, eventData);
        }
    }

    private void downloadFailed(DownloadFailedEvent eventData) {
        try {
            transfers.failedByPeer(eventData.username(), eventData.filename());
        } catch (Throwable failure) {
            diagnostic.warning("Failed to mark download(s) failed: " + Failures.message(failure), failure);
        } finally {
            events.publish(Kind.DOWNLOAD_FAILED, eventData);
        }
    }

    private void connectInternal(
            String requestedAddress,
            InetSocketAddress requestedEndpoint,
            String requestedUsername,
            String password,
            CancellationSignal cancellationSignal) {
        try {
            // The lock is never held on the failing path, so there is nothing
            // to unlock; acquisition stays outside the try for that reason.
            Locks.acquire(stateLock, cancellationSignal);
        } catch (InterruptedException interrupted) {
            throw new InterruptedOperationException("The connect invocation was interrupted", interrupted);
        } catch (RuntimeException failure) {
            throw reportConnectFailure(failure);
        }

        try {
            if (!state.isLoggedIn()) {
                performConnect(requestedAddress, requestedEndpoint, requestedUsername, password, cancellationSignal);
            }
        } catch (Throwable failure) {
            throw reportConnectFailure(failure);
        } finally {
            stateLock.unlock();
        }
    }

    /** Classifies a connect failure and tears the connection down. */
    private RuntimeException reportConnectFailure(Throwable cause) {
        Throwable reported;
        if (cause instanceof TimeoutException) {
            // A lapsed deadline cannot be thrown as itself past here: the
            // checked TimeoutException has no place on an unchecked surface.
            reported = new NoResponseException(Failures.message(cause), cause);
        } else if (cause instanceof LoginRejectedException || cause instanceof CancellationException) {
            reported = cause;
        } else {
            reported = new SoulseekClientException("Failed to connect: " + Failures.message(cause), cause);
        }
        disconnect(Failures.message(reported), asException(reported));
        throw Failures.surface(reported);
    }

    private void performConnect(
            String requestedAddress,
            InetSocketAddress requestedEndpoint,
            String requestedUsername,
            String password,
            CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        changeState(SoulseekClientState.CONNECTING, "Connecting", null);

        if (options.enableListener()) {
            listener = clientListenerFactory.create(
                    options.listenIpAddress(), options.listenPort(), options.incomingConnectionOptions());
            listener.subscribe(listenerHandler::handleConnection);
            listener.start();
        }

        MessageConnection connection = connectionFactory.getServerConnection(
                requestedEndpoint,
                eventData -> changeState(SoulseekClientState.CONNECTED, "Connected to " + ipEndpoint, null),
                eventData -> disconnect(eventData.message(), eventData.exception()),
                serverMessageHandler::handleMessageRead,
                serverMessageHandler::handleMessageWritten,
                options.serverConnectionOptions());

        server.connection(connection);
        connection.connect(cancellationSignal);
        address = requestedAddress;
        ipEndpoint = requestedEndpoint;
        changeState(SoulseekClientState.LOGGING_IN, "Logging in", null);
        login(requestedUsername, password, cancellationSignal);
    }

    private void login(String requestedUsername, String password, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        // Registered before the login bytes go out: the server answers a login
        // as fast as anything on this protocol.
        Wait<LoginResponse> loginWait = waiter.register(
                new WaitKey(MessageCode.Server.LOGIN),
                LoginResponse.class,
                waiter.getDefaultTimeout(),
                cancellationSignal);

        ByteArrayOutputStream loginMessages = new ByteArrayOutputStream();
        loginMessages.writeBytes(new LoginRequest(minorVersion, requestedUsername, password).toByteArray());
        loginMessages.writeBytes(new SetListenPortCommand(options.listenPort()).toByteArray());

        server.writeBytes(loginMessages.toByteArray(), cancellationSignal);
        LoginResponse response = loginWait.await();
        if (!response.isSucceeded()) {
            throw new LoginRejectedException("The server rejected login attempt: " + response.getMessage());
        }
        serverInfo = serverInfo.with(null, null, null, response.isSupporter());
        events.publish(Kind.SERVER_INFO_RECEIVED, serverInfo);
        server.username(requestedUsername);
        changeState(SoulseekClientState.LOGGED_IN, "Logged in", null);
        sendConfigurationMessages(cancellationSignal);
    }

    void sendConfigurationMessages(CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        server.write(new SetListenPortCommand(options.listenPort()), cancellationSignal);
        server.write(new PrivateRoomToggle(options.acceptPrivateRoomInvitations()), cancellationSignal);
        // Our own statistics, for the upload average the server keeps for this
        // account: search responses advertise it, and the server only says it
        // when asked. The response arrives as a statistics event naming us,
        // which is what routes it to the transfer domain.
        server.write(
                new dev.slsk.internal.messaging.messages.UserStatisticsRequest(server.username()), cancellationSignal);
        distributedConnectionManager.updateStatus(cancellationSignal);
    }

    // ---- reconfiguration --------------------------------------------------
    //
    // Applying an option patch to a running client: swapping the listener,
    // resizing the rate-limit buckets, and deciding whether the change needs a
    // reconnect. This lived in a class of its own that took the client whole,
    // because routing it through a seam interface would have meant a dozen
    // accessors for one caller. Now that the client is an engine rather than an
    // API, it is simply the engine's own work.
    //
    // No facet exposes this: options are set at build time. It stays because it
    // is the machinery a runtime speed limit needs, and Downloads.policy will.

    private boolean performReconfigureOptions(SoulseekClientOptionsPatch patch, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        boolean connected = isConnectedAndLoggedIn();
        boolean enableDistributedNetworkChanged = patch.enableDistributedNetwork()
                .filter(value -> value != options.enableDistributedNetwork())
                .isPresent();
        boolean acceptDistributedChildrenChanged = patch.acceptDistributedChildren()
                .filter(value -> value != options.acceptDistributedChildren())
                .isPresent();
        boolean distributedConnectionOptionsChanged = patch.distributedConnectionOptions()
                .filter(value -> value != options.distributedConnectionOptions())
                .isPresent();
        boolean distributedNetworkWasDisabled = enableDistributedNetworkChanged
                && !patch.enableDistributedNetwork().orElseThrow();
        boolean distributedChildrenWereDisabled = acceptDistributedChildrenChanged
                && !patch.acceptDistributedChildren().orElseThrow();
        boolean reconnectRequired = connected
                && (distributedNetworkWasDisabled
                        || distributedChildrenWereDisabled
                        || distributedConnectionOptionsChanged);
        boolean serverConnectionOptionsChanged = patch.serverConnectionOptions()
                .filter(value -> value != options.serverConnectionOptions())
                .isPresent();
        if (connected && serverConnectionOptionsChanged) {
            reconnectRequired = true;
        }

        boolean enableListenerChanged = patch.enableListener()
                .filter(value -> value != options.enableListener())
                .isPresent();
        boolean listenAddressChanged = patch.listenIpAddress()
                .filter(value -> !value.equals(options.listenIpAddress()))
                .isPresent();
        boolean listenPortChanged = patch.listenPort()
                .filter(value -> value != options.listenPort())
                .isPresent();
        boolean incomingConnectionOptionsChanged = patch.incomingConnectionOptions()
                .filter(value -> value != options.incomingConnectionOptions())
                .isPresent();

        if (enableListenerChanged || listenAddressChanged || listenPortChanged || incomingConnectionOptionsChanged) {
            boolean wasListening = listener != null && listener.isListening();
            if (listener != null) {
                listener.stop();
            }
            listener = null;
            options = options.with(listenerPatch(patch));
            if (wasListening && options.enableListener()) {
                listener = clientListenerFactory.create(
                        options.listenIpAddress(), options.listenPort(), options.incomingConnectionOptions());
                listener.subscribe(listenerHandler::handleConnection);
                listener.start();
            }
        }

        boolean maximumUploadSpeedChanged = patch.maximumUploadSpeed()
                .filter(value -> value != options.maximumUploadSpeed())
                .isPresent();
        boolean maximumDownloadSpeedChanged = patch.maximumDownloadSpeed()
                .filter(value -> value != options.maximumDownloadSpeed())
                .isPresent();
        options = options.with(patch);

        if (maximumUploadSpeedChanged) {
            uploadTokenBucket.setCapacity((options.maximumUploadSpeed() * 1024L) / 10);
        }
        if (maximumDownloadSpeedChanged) {
            downloadTokenBucket.setCapacity((options.maximumDownloadSpeed() * 1024L) / 10);
        }

        diagnostic.info("Options reconfigured successfully");
        if (!isConnectedAndLoggedIn()) {
            return false;
        }
        diagnostic.debug("Updating server with latest configuration");
        sendConfigurationMessages(cancellationSignal);
        if (reconnectRequired) {
            diagnostic.warning("Server reconnect required for options " + "to fully take effect");
        }
        return reconnectRequired;
    }

    private boolean reconfigureOptionsInternal(
            SoulseekClientOptionsPatch patch, CancellationSignal cancellationSignal) {
        try {
            // Never held on the failing path, so nothing to unlock.
            Locks.acquire(stateLock, cancellationSignal);
        } catch (InterruptedException interrupted) {
            throw new InterruptedOperationException("The reconfiguration invocation was interrupted", interrupted);
        } catch (RuntimeException failure) {
            throw reportReconfigureFailure(failure);
        }
        try {
            return performReconfigureOptions(patch, cancellationSignal);
        } catch (Throwable failure) {
            throw reportReconfigureFailure(failure);
        } finally {
            stateLock.unlock();
        }
    }

    /** Classifies a reconfiguration failure, which is never rolled back. */
    private RuntimeException reportReconfigureFailure(Throwable cause) {
        if (cause instanceof CancellationException || cause instanceof TimeoutException) {
            throw Failures.surface(cause);
        }
        throw new SoulseekClientException(
                "Failed to reconfigure options: "
                        + Failures.message(cause)
                        + ".  Any successful reconfiguration has not "
                        + "been rolled back; retry with the same patch "
                        + "until successful or consider this as a "
                        + "fatal Exception",
                cause);
    }

    private static SoulseekClientOptionsPatch listenerPatch(SoulseekClientOptionsPatch patch) {
        SoulseekClientOptionsPatch.Builder builder = SoulseekClientOptionsPatch.builder();
        patch.enableListener().ifPresent(builder::enableListener);
        patch.listenIpAddress().ifPresent(builder::listenIpAddress);
        patch.listenPort().ifPresent(builder::listenPort);
        patch.incomingConnectionOptions().ifPresent(builder::incomingConnectionOptions);
        return builder.build();
    }

    boolean isConnectedAndLoggedIn() {
        return state.isLoggedIn();
    }

    private static Exception asException(Throwable failure) {
        if (failure instanceof Exception exception) {
            return exception;
        }
        return new RuntimeException(failure);
    }

    // ---- what the in-package collaborators still read here ----------------

    /** The periodic endpoint-semaphore sweep; exposed for tests. */
    void cleanupUserEndpointSemaphores() {
        users.cleanupUserEndpointSemaphores();
    }

    void reportBrowseProgress(
            String requestedUsername,
            BrowseOptions operationOptions,
            long bytesTransferred,
            long size,
            AtomicBoolean completionEventFired) {
        BrowseProgressUpdatedEvent eventData =
                new BrowseProgressUpdatedEvent(requestedUsername, bytesTransferred, size);
        if (Double.compare(eventData.percentComplete(), 100.0) == 0) {
            completionEventFired.set(true);
        }
        if (operationOptions.progressUpdated() != null) {
            operationOptions
                    .progressUpdated()
                    .accept(new BrowseProgress(
                            eventData.username(),
                            eventData.bytesTransferred(),
                            eventData.bytesRemaining(),
                            eventData.percentComplete(),
                            eventData.size()));
        }
        events.publish(Kind.BROWSE_PROGRESS_UPDATED, eventData);
    }

    TokenFactory getTokenFactory() {
        return tokenFactory;
    }

    Scheduler getScheduler() {
        return scheduler;
    }

    NetworkExecutor getNetworkExecutor() {
        return networkExecutor;
    }

    <T> void publishEvent(EngineEvents.Kind kind, T eventData) {
        events.publish(kind, eventData);
    }

    java.util.Map<Integer, TransferInternal> getDownloadRegistry() {
        return transfers.downloads();
    }

    java.util.Map<Integer, TransferInternal> getUploadRegistry() {
        return transfers.uploads();
    }

    SoulseekClientOptions getClientOptions() {
        return options;
    }

    DiagnosticSink getDiagnostic() {
        return diagnostic;
    }

    @FunctionalInterface
    interface ClientListenerFactory {
        Listener create(InetAddress ipAddress, int port, ConnectionOptions connectionOptions);
    }

    public void connect(String username, String password) {
        connectOperation(username, password);
    }

    public void connect(String username, String password, CancellationSignal cancellationSignal) {
        connectOperation(username, password, cancellationSignal);
    }

    public void connect(String address, int port, String username, String password) {
        connectOperation(address, port, username, password);
    }

    public void connect(
            String address, int port, String username, String password, CancellationSignal cancellationSignal) {
        connectOperation(address, port, username, password, cancellationSignal);
    }

    public Boolean reconfigureOptions(SoulseekClientOptionsPatch patch) {
        return reconfigureOptionsOperation(patch);
    }

    public Boolean reconfigureOptions(SoulseekClientOptionsPatch patch, CancellationSignal cancellationSignal) {
        return reconfigureOptionsOperation(patch, cancellationSignal);
    }

    RoomRegistry rooms() {
        return rooms;
    }

    UserDirectory users() {
        return users;
    }

    ServerLink server() {
        return server;
    }

    SearchDomain searches() {
        return searchDomain;
    }

    TransferDomain transfers() {
        return transfers;
    }
}
