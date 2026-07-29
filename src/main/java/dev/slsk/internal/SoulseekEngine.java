// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.CancellationSignal;
import dev.slsk.UserProfile;
import dev.slsk.exceptions.AddressException;
import dev.slsk.exceptions.KickedFromServerException;
import dev.slsk.exceptions.ListenException;
import dev.slsk.exceptions.LoginRejectedException;
import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferReportedFailedException;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.common.CommonUtils;
import dev.slsk.internal.common.DefaultWaiter;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.IOAdapter;
import dev.slsk.internal.common.Permits;
import dev.slsk.internal.common.Scheduler;
import dev.slsk.internal.common.TokenBucket;
import dev.slsk.internal.common.TokenFactory;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
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
import dev.slsk.internal.messaging.handlers.DistributedMessageHandlerClient;
import dev.slsk.internal.messaging.handlers.PeerMessageHandler;
import dev.slsk.internal.messaging.handlers.PeerMessageHandlerClient;
import dev.slsk.internal.messaging.handlers.ServerMessageEvent;
import dev.slsk.internal.messaging.handlers.ServerMessageHandler;
import dev.slsk.internal.messaging.handlers.ServerMessageHandlerClient;
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
import dev.slsk.spi.UploadPolicy;
import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * What runs underneath the facets.
 *
 * <p>This was {@code DefaultSoulseekClient}, the implementation of a public
 * interface with a hundred and eighty-five methods. Both are gone. What is left
 * is not a client — nothing outside this package can name it, let alone hold
 * one — but the machinery a client needs: the connection and login state
 * machine, the wiring of every component, {@link EngineContext} for the
 * collaborators to delegate through, and {@link EngineEvents} for the facets to
 * subscribe to.
 *
 * <p>It also answers the callbacks of every message handler and connection
 * manager, which is why it implements so many of their interfaces: incoming
 * messages are dispatched against registries it owns.
 */
final class SoulseekEngine
        implements AutoCloseable,
                EngineContext,
                UploadAdmission.Host,
                DistributedMessageHandlerClient,
                PeerMessageHandlerClient,
                ServerMessageHandlerClient {

    private static final int MAJOR_VERSION = 170;
    private static final String DEFAULT_ADDRESS = "server.slsknet.org";
    private static final int DEFAULT_PORT = 2271;

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
    /**
     * What the engine tells the facets. Constructed before the diagnostic sink,
     * because the sink raises through it.
     */
    private final EngineEvents events = new EngineEvents(this::reportListenerFault);

    volatile ClientListenerFactory clientListenerFactory = SocketListener::new;
    private volatile ShareCatalog catalog = ShareCatalog.empty();
    private volatile UserProfile profile = UserProfile.empty();
    private volatile UploadPolicy uploadPolicy = UploadPolicy.standard(2, 1);
    private volatile UploadAdmission uploadAdmission = new UploadAdmission(this);

    /**
     * How a running upload is stopped.
     *
     * <p>Uploads are started by us on a peer's behalf, so nothing outside holds
     * a signal for one. Keeping the controller here is what makes
     * {@code Uploads.cancel} able to do anything at all.
     */
    private final java.util.Map<dev.slsk.TransferId, dev.slsk.CancellationController> uploadCancellations =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Who the server said has bought privileges.
     *
     * <p>Sent once on login as a list, and the only way to know: there is no
     * per-user query for it. Kept because privileged users jump the upload
     * queue, which is protocol-mandated rather than a matter of taste.
     */
    private volatile java.util.Set<String> privilegedUsers = java.util.Set.of();

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

    /** User info, presence and browsing, split out; see UserDirectory. */
    private final UserDirectory users;

    /** The server connection and everything said over it; see ServerLink. */
    private final ServerLink server;

    /** Searches and the registry of the ones in flight; see SearchDomain. */
    private final SearchDomain searchDomain;

    /** Transfer orchestration, split out; see TransferEngine. */
    private final TransferEngine transfers;

    volatile Listener listener;
    volatile String address;
    volatile InetSocketAddress ipEndpoint;
    private volatile ServerInfo serverInfo = new ServerInfo();
    volatile SoulseekClientState state = SoulseekClientState.DISCONNECTED;
    private volatile Map<Integer, TransferInternal> downloads = new ConcurrentHashMap<>();
    private volatile Map<Integer, TransferInternal> uploads = new ConcurrentHashMap<>();
    private final Map<String, Semaphore> uploadSemaphores = new ConcurrentHashMap<>();

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
        this.scheduler = new Scheduler("soulseek-client-timer");
        this.waiter = waiter == null ? new DefaultWaiter(this.options.getMessageTimeout(), scheduler) : waiter;
        // Before every component that writes to the server, because they are
        // built with it rather than reaching back through the engine for it.
        diagnostic = diagnosticFactory == null
                ? new FilteringDiagnosticSink(
                        this.options.getMinimumDiagnosticLevel(),
                        eventData -> events.raise(Kind.DIAGNOSTIC_GENERATED, eventData))
                : diagnosticFactory;
        this.server = new ServerLink(this.waiter, diagnostic, () -> state);
        this.server.connection(serverConnection);
        this.rooms = new RoomRegistry(this, server);
        this.users = new UserDirectory(this, server);
        this.searchDomain = new SearchDomain(this, server);
        this.transfers = new TransferEngine(this, server);
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
                        this::getShareCatalog,
                        server::username)
                : searchResponder;
        this.peerMessageHandler = peerMessageHandler == null ? new DefaultPeerMessageHandler(this) : peerMessageHandler;
        this.distributedMessageHandler = distributedMessageHandler == null
                ? new DefaultDistributedMessageHandler(this)
                : distributedMessageHandler;
        this.peerConnectionManager = peerConnectionManager == null
                ? new PeerNetwork(this::getOptions, server, this.waiter, this.tokenFactory, this.peerMessageHandler)
                : peerConnectionManager;
        this.distributedConnectionManager = distributedConnectionManager == null
                ? new DistributedNetwork(
                        this::getOptions,
                        server,
                        this.waiter,
                        this.tokenFactory,
                        this::getDistributedMessageHandler,
                        null,
                        null,
                        scheduler)
                : distributedConnectionManager;
        this.serverMessageHandler =
                serverMessageHandler == null ? new DefaultServerMessageHandler(this) : serverMessageHandler;

        bindEvents();

        scheduler.scheduleAtFixedRate(users::cleanupUserEndpointSemaphores, 5, 5, TimeUnit.MINUTES);
        scheduler.scheduleAtFixedRate(this::cleanupUploadSemaphores, 15, 15, TimeUnit.MINUTES);
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
    @Override
    public ShareCatalog getShareCatalog() {
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
    @Override
    public UserProfile getProfile() {
        return profile;
    }

    /**
     * Returns who we serve and in what order.
     *
     * @return the upload policy, never {@code null}
     */
    @Override
    public UploadPolicy getUploadPolicy() {
        return uploadPolicy;
    }

    @Override
    public UploadPolicy uploadPolicy() {
        return uploadPolicy;
    }

    @Override
    public java.util.Map<Integer, dev.slsk.internal.transfer.TransferInternal> uploads() {
        return getUploadRegistry();
    }

    @Override
    public DiagnosticSink diagnostic() {
        return diagnostic;
    }

    /**
     * Sets who we serve and in what order.
     *
     * @param value the policy, or {@code null} for the standard one
     */
    void setUploadPolicy(UploadPolicy value) {
        uploadPolicy = value == null ? UploadPolicy.standard(2, 1) : value;
    }

    /**
     * Returns what admits or refuses a peer's request.
     *
     * @return the admission
     */
    @Override
    public UploadAdmission getUploadAdmission() {
        return uploadAdmission;
    }

    /**
     * Stops a running upload. A no-op for one that has already finished.
     *
     * @param id which upload
     * @return whether there was one to stop
     */
    boolean cancelUpload(dev.slsk.TransferId id) {
        dev.slsk.CancellationController cancellation = uploadCancellations.remove(id);
        if (cancellation == null) {
            return false;
        }
        cancellation.cancel();
        return true;
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
    void setDownloadSpeedLimit(dev.slsk.Bandwidth limit) {
        downloadTokenBucket.setCapacity(
                limit == null || limit.isUnlimited() ? Long.MAX_VALUE / 16 : Math.max(1, limit.bytesPerSecond() / 10));
    }

    /** Answers a peer's unsolicited offer of a file. */
    @FunctionalInterface
    interface DownloadOffers {
        PeerMessageHandlerClient.OfferDisposition offered(
                String username, String filename, dev.slsk.internal.messaging.messages.TransferRequest offer);
    }

    /**
     * Who decides what an offered file is.
     *
     * <p>The download queue, once the downloads facet installs itself. Until
     * then every offer is unknown, which is the honest answer: without a queue
     * there is nothing an offer could match beyond the live transfers the
     * handler already checked.
     */
    private volatile DownloadOffers downloadOffers =
            (username, filename, offer) -> PeerMessageHandlerClient.OfferDisposition.UNKNOWN;

    void downloadOffers(DownloadOffers value) {
        this.downloadOffers = Objects.requireNonNull(value, "downloadOffers");
    }

    @Override
    public PeerMessageHandlerClient.OfferDisposition offerDownload(
            String username, String filename, dev.slsk.internal.messaging.messages.TransferRequest offer) {
        return downloadOffers.offered(username, filename, offer);
    }

    /**
     * Serves a file to a peer whose request the policy allowed.
     *
     * <p>Nothing did this before 1.0. The old surface accepted the request in
     * {@code EnqueueDownloadCallback} and left the application to call {@code
     * upload(...)} itself, which is why "uploads are requested by peers and
     * admitted by the upload policy" has to mean the library serves them —
     * otherwise the capability is disposed of and nothing takes it over.
     *
     * <p>The bytes come from {@link ShareCatalog#resolve}, so a peer can only
     * ever receive what the catalog agrees it may have, checked against the
     * share root rather than trusted from the request.
     *
     * @param user who asked
     * @param path the file they asked for
     */
    @Override
    public void serveUpload(dev.slsk.Username user, String path) {
        dev.slsk.internal.common.NetworkExecutor.runAsync(() -> {
            java.util.Optional<dev.slsk.spi.ResolvedFile> resolved;
            try {
                resolved = catalog.resolve(user, path);
            } catch (RuntimeException failure) {
                diagnostic.warning("The share catalog failed to resolve " + path, failure);
                return;
            }
            if (resolved.isEmpty()) {
                // Allowed by policy but not by the catalog. One answer for every
                // rejection, so the reply cannot become a filesystem oracle.
                uploadAdmission.forget(user, path);
                return;
            }
            dev.slsk.spi.ResolvedFile file = resolved.get();
            int token = getNextToken();
            dev.slsk.TransferId id = dev.slsk.TransferId.of("UPLOAD:" + token);
            dev.slsk.CancellationController cancellation = new dev.slsk.CancellationController();
            uploadCancellations.put(id, cancellation);
            try {
                transfers
                        .upload(
                                user.value(),
                                path,
                                file.size(),
                                offset -> {
                                    try {
                                        return java.nio.channels.Channels.newInputStream(file.open(offset));
                                    } catch (java.io.IOException failure) {
                                        throw new java.io.UncheckedIOException(failure);
                                    }
                                },
                                token,
                                null,
                                cancellation.getSignal())
                        .whenComplete((transfer, failure) -> {
                            uploadCancellations.remove(id);
                            uploadAdmission.forget(user, path);
                            if (failure == null && transfer != null) {
                                uploadAdmission.served(user, transfer.getBytesTransferred());
                            }
                        });
            } catch (RuntimeException failure) {
                uploadCancellations.remove(id);
                uploadAdmission.forget(user, path);
                diagnostic.warning("Failed to start an upload of " + path + " to " + user, failure);
            }
        });
    }

    /**
     * Returns whether the server said a user has bought privileges.
     *
     * @param username who
     * @return whether they are privileged
     */
    @Override
    public boolean isPrivileged(String username) {
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
    public final SoulseekClientState getState() {
        return state;
    }

    /** Returns the logged-in username, or {@code null}. */
    @Override
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
            throw new IllegalArgumentException("port must be between 0 and 65535 (specified: " + requestedPort + ")");
        }
        CommonUtils.requireNonEmpty(requestedUsername, "username");
        CommonUtils.requireNonEmpty(password, "password");
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
                    "Failed to resolve address '" + requestedAddress + "': " + Failures.message(failure), failure);
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
        distributedConnectionManager.removeAndDisposeAll();
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
        disconnect("Client is being disposed", new IllegalStateException("The client is closed"));
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
        scheduler.close();
    }

    @Override
    public final Waiter getWaiter() {
        return waiter;
    }

    @Override
    public final Map<Integer, SearchInternal> getSearches() {
        return searchDomain.registry();
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

    public final MessageConnection getServerConnection() {
        return server.connection();
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
        server.connection(value);
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
        searchDomain.registry(value);
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
        events.raise(Kind.STATE_CHANGED, new SoulseekClientStateChangedEvent(previousState, state, message, exception));
        if (state.equals(SoulseekClientState.CONNECTED)) {
            events.raise(Kind.CONNECTED, null);
        } else if (state.equals(SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGED_IN))) {
            events.raise(Kind.LOGGED_IN, null);
        } else if (state.equals(SoulseekClientState.DISCONNECTED)) {
            events.raise(Kind.DISCONNECTED, new SoulseekClientDisconnectedEvent(message, exception));
        }
    }

    private void bindEvents() {
        listenerHandler.addDiagnosticGeneratedListener(
                (sender, eventData) -> events.raise(Kind.DIAGNOSTIC_GENERATED, eventData));
        searchResponder.addDiagnosticGeneratedListener(
                (sender, eventData) -> events.raise(Kind.DIAGNOSTIC_GENERATED, eventData));
        searchResponder.addRequestReceivedListener(
                (sender, eventData) -> events.raise(Kind.SEARCH_REQUEST_RECEIVED, eventData));
        searchResponder.addResponseDeliveredListener(
                (sender, eventData) -> events.raise(Kind.SEARCH_RESPONSE_DELIVERED, eventData));
        searchResponder.addResponseDeliveryFailedListener(
                (sender, eventData) -> events.raise(Kind.SEARCH_RESPONSE_DELIVERY_FAILED, eventData));

        peerMessageHandler.addDiagnosticGeneratedListener(
                (sender, eventData) -> events.raise(Kind.DIAGNOSTIC_GENERATED, eventData));
        peerMessageHandler.addDownloadDeniedListener((sender, eventData) -> downloadDenied(eventData));
        peerMessageHandler.addDownloadFailedListener((sender, eventData) -> downloadFailed(eventData));
        distributedMessageHandler.addDiagnosticGeneratedListener(
                (sender, eventData) -> events.raise(Kind.DIAGNOSTIC_GENERATED, eventData));
        peerConnectionManager.addDiagnosticGeneratedListener(
                (sender, eventData) -> events.raise(Kind.DIAGNOSTIC_GENERATED, eventData));
        distributedConnectionManager.addDiagnosticGeneratedListener(
                (sender, eventData) -> events.raise(Kind.DIAGNOSTIC_GENERATED, eventData));
        distributedConnectionManager.addPromotedToBranchRootListener(
                (sender, eventData) -> events.raise(Kind.PROMOTED_TO_DISTRIBUTED_BRANCH_ROOT, null));
        distributedConnectionManager.addDemotedFromBranchRootListener(
                (sender, eventData) -> events.raise(Kind.DEMOTED_FROM_DISTRIBUTED_BRANCH_ROOT, null));
        distributedConnectionManager.addParentAdoptedListener(
                (sender, eventData) -> events.raise(Kind.DISTRIBUTED_PARENT_ADOPTED, eventData));
        distributedConnectionManager.addParentDisconnectedListener(
                (sender, eventData) -> events.raise(Kind.DISTRIBUTED_PARENT_DISCONNECTED, eventData));
        distributedConnectionManager.addChildAddedListener(
                (sender, eventData) -> events.raise(Kind.DISTRIBUTED_CHILD_ADDED, eventData));
        distributedConnectionManager.addChildDisconnectedListener(
                (sender, eventData) -> events.raise(Kind.DISTRIBUTED_CHILD_DISCONNECTED, eventData));
        distributedConnectionManager.addStateChangedListener(
                (sender, eventData) -> events.raise(Kind.DISTRIBUTED_NETWORK_STATE_CHANGED, eventData));

        serverMessageHandler.addDiagnosticGeneratedListener(
                (sender, eventData) -> events.raise(Kind.DIAGNOSTIC_GENERATED, eventData));
        bindServerEvents();
    }

    private void bindServerEvents() {
        forwardServer(ServerMessageEvent.USER_CANNOT_CONNECT, Kind.USER_CANNOT_CONNECT);
        forwardServer(ServerMessageEvent.USER_STATUS_CHANGED, Kind.USER_STATUS_CHANGED);
        forwardServer(ServerMessageEvent.USER_STATISTICS_CHANGED, Kind.USER_STATISTICS_CHANGED);
        forwardServer(ServerMessageEvent.PRIVATE_MESSAGE_RECEIVED, Kind.PRIVATE_MESSAGE_RECEIVED);
        forwardServer(ServerMessageEvent.PRIVATE_ROOM_MEMBERSHIP_ADDED, Kind.PRIVATE_ROOM_MEMBERSHIP_ADDED);
        forwardServer(ServerMessageEvent.PRIVATE_ROOM_MEMBERSHIP_REMOVED, Kind.PRIVATE_ROOM_MEMBERSHIP_REMOVED);
        forwardServer(
                ServerMessageEvent.PRIVATE_ROOM_MODERATED_USER_LIST_RECEIVED,
                Kind.PRIVATE_ROOM_MODERATED_USER_LIST_RECEIVED);
        forwardServer(ServerMessageEvent.PRIVATE_ROOM_MODERATION_ADDED, Kind.PRIVATE_ROOM_MODERATION_ADDED);
        forwardServer(ServerMessageEvent.PRIVATE_ROOM_MODERATION_REMOVED, Kind.PRIVATE_ROOM_MODERATION_REMOVED);
        forwardServer(ServerMessageEvent.PRIVATE_ROOM_USER_LIST_RECEIVED, Kind.PRIVATE_ROOM_USER_LIST_RECEIVED);
        serverMessageHandler.<java.util.List<String>>addListener(
                ServerMessageEvent.PRIVILEGED_USER_LIST_RECEIVED, (sender, eventData) -> {
                    privilegedUsers = eventData == null ? java.util.Set.of() : java.util.Set.copyOf(eventData);
                    events.raise(Kind.PRIVILEGED_USER_LIST_RECEIVED, eventData);
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
        serverMessageHandler.<ServerInfo>addListener(ServerMessageEvent.SERVER_INFO_RECEIVED, (sender, eventData) -> {
            serverInfo = serverInfo.with(
                    eventData.getParentMinSpeed(),
                    eventData.getParentSpeedRatio(),
                    eventData.getWishlistInterval(),
                    eventData.isSupporter());
            events.raise(Kind.SERVER_INFO_RECEIVED, serverInfo);
        });
        serverMessageHandler.<Void>addListener(ServerMessageEvent.KICKED_FROM_SERVER, (sender, eventData) -> {
            diagnostic.info("Kicked from server.");
            events.raise(Kind.KICKED_FROM_SERVER, null);
            disconnect("Kicked from server", new KickedFromServerException());
        });
    }

    private <T> void forwardServer(ServerMessageEvent source, EngineEvents.Kind target) {
        serverMessageHandler.<T>addListener(source, (sender, eventData) -> events.raise(target, eventData));
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
            diagnostic.warning("Failed to mark download(s) rejected: " + Failures.message(failure), failure);
        } finally {
            events.raise(Kind.DOWNLOAD_DENIED, eventData);
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
            diagnostic.warning("Failed to mark download(s) failed: " + Failures.message(failure), failure);
        } finally {
            events.raise(Kind.DOWNLOAD_FAILED, eventData);
        }
    }

    private void connectInternal(
            String requestedAddress,
            InetSocketAddress requestedEndpoint,
            String requestedUsername,
            String password,
            CancellationSignal cancellationSignal) {
        try {
            // The permit is never held on the failing path, so there is nothing
            // to release; the acquire stays outside the try for that reason.
            Permits.acquire(stateSemaphore, cancellationSignal);
        } catch (RuntimeException failure) {
            throw reportConnectFailure(failure);
        }

        try {
            if (!state.contains(SoulseekClientState.CONNECTED) || !state.contains(SoulseekClientState.LOGGED_IN)) {
                performConnect(requestedAddress, requestedEndpoint, requestedUsername, password, cancellationSignal);
            }
        } catch (Throwable failure) {
            throw reportConnectFailure(failure);
        } finally {
            stateSemaphore.release();
        }
    }

    /** Classifies a connect failure and tears the connection down. */
    private RuntimeException reportConnectFailure(Throwable failure) {
        Throwable cause = Failures.unwrap(failure);
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
            CancellationSignal cancellationSignal) {
        try {
            changeState(SoulseekClientState.CONNECTING, "Connecting", null);

            if (options.isEnableListener()) {
                listener = clientListenerFactory.create(
                        options.getListenIpAddress(), options.getListenPort(), options.getIncomingConnectionOptions());
                listener.addAcceptedListener(listenerHandler::handleConnection);
                listener.start();
            }

            MessageConnection connection = connectionFactory.getServerConnection(
                    requestedEndpoint,
                    (sender, eventData) ->
                            changeState(SoulseekClientState.CONNECTED, "Connected to " + ipEndpoint, null),
                    (sender, eventData) -> disconnect(eventData.getMessage(), eventData.getException()),
                    serverMessageHandler::handleMessageRead,
                    serverMessageHandler::handleMessageWritten,
                    options.getServerConnectionOptions());

            server.connection(connection);
            connection.connect(cancellationSignal);
            address = requestedAddress;
            ipEndpoint = requestedEndpoint;
            changeState(SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGING_IN), "Logging in", null);
            login(requestedUsername, password, cancellationSignal);
        } catch (Throwable failure) {
            throw Failures.propagate(failure);
        }
    }

    private void login(String requestedUsername, String password, CancellationSignal cancellationSignal) {
        // Registered before the login bytes go out: the server answers a login
        // as fast as anything on this protocol.
        Wait<LoginResponse> loginWait =
                waiter.register(new WaitKey(MessageCode.Server.LOGIN), LoginResponse.class, null, cancellationSignal);

        ByteArrayOutputStream loginMessages = new ByteArrayOutputStream();
        loginMessages.writeBytes(new LoginRequest(minorVersion, requestedUsername, password).toByteArray());
        loginMessages.writeBytes(new SetListenPortCommand(options.getListenPort()).toByteArray());

        server.writeBytes(loginMessages.toByteArray(), cancellationSignal);
        LoginResponse response = loginWait.await();
        if (!response.isSucceeded()) {
            throw new LoginRejectedException("The server rejected login attempt: " + response.getMessage());
        }
        serverInfo = serverInfo.with(null, null, null, response.isSupporter());
        events.raise(Kind.SERVER_INFO_RECEIVED, serverInfo);
        server.username(requestedUsername);
        changeState(SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGED_IN), "Logged in", null);
        sendConfigurationMessages(cancellationSignal);
    }

    void sendConfigurationMessages(CancellationSignal cancellationSignal) {
        server.write(new SetListenPortCommand(options.getListenPort()), cancellationSignal);
        server.write(new PrivateRoomToggle(options.isAcceptPrivateRoomInvitations()), cancellationSignal);
        distributedConnectionManager.updateStatusAsync(cancellationSignal);
    }

    // ---- reconfiguration --------------------------------------------------
    //
    // Applying an option patch to a running client: swapping the listener,
    // resizing the rate-limit buckets, and deciding whether the change needs a
    // reconnect. This lived in a class of its own that took the client whole,
    // because routing it through EngineContext would have meant a dozen
    // accessors for one caller. Now that the client is an engine rather than an
    // API, it is simply the engine's own work.
    //
    // No facet exposes this: options are set at build time. It stays because it
    // is the machinery a runtime speed limit needs, and Downloads.policy will.

    private boolean performReconfigureOptions(SoulseekClientOptionsPatch patch, CancellationSignal cancellationSignal) {
        boolean connected = isConnectedAndLoggedIn();
        boolean enableDistributedNetworkChanged = patch.getEnableDistributedNetwork() != null
                && patch.getEnableDistributedNetwork() != options.isEnableDistributedNetwork();
        boolean acceptDistributedChildrenChanged = patch.getAcceptDistributedChildren() != null
                && patch.getAcceptDistributedChildren() != options.isAcceptDistributedChildren();
        boolean distributedConnectionOptionsChanged = patch.getDistributedConnectionOptions() != null
                && patch.getDistributedConnectionOptions() != options.getDistributedConnectionOptions();
        boolean distributedNetworkWasDisabled = enableDistributedNetworkChanged && !patch.getEnableDistributedNetwork();
        boolean distributedChildrenWereDisabled =
                acceptDistributedChildrenChanged && !patch.getAcceptDistributedChildren();
        boolean reconnectRequired = connected
                && (distributedNetworkWasDisabled
                        || distributedChildrenWereDisabled
                        || distributedConnectionOptionsChanged);
        boolean serverConnectionOptionsChanged = patch.getServerConnectionOptions() != null
                && patch.getServerConnectionOptions() != options.getServerConnectionOptions();
        if (connected && serverConnectionOptionsChanged) {
            reconnectRequired = true;
        }

        boolean enableListenerChanged =
                patch.getEnableListener() != null && patch.getEnableListener() != options.isEnableListener();
        boolean listenAddressChanged = patch.getListenIpAddress() != null
                && !patch.getListenIpAddress().equals(options.getListenIpAddress());
        boolean listenPortChanged = patch.getListenPort() != null && patch.getListenPort() != options.getListenPort();
        boolean incomingConnectionOptionsChanged = patch.getIncomingConnectionOptions() != null
                && patch.getIncomingConnectionOptions() != options.getIncomingConnectionOptions();

        if (enableListenerChanged || listenAddressChanged || listenPortChanged || incomingConnectionOptionsChanged) {
            boolean wasListening = listener != null && listener.isListening();
            if (listener != null) {
                listener.stop();
            }
            listener = null;
            options = options.with(listenerPatch(patch));
            if (wasListening && options.isEnableListener()) {
                listener = clientListenerFactory.create(
                        options.getListenIpAddress(), options.getListenPort(), options.getIncomingConnectionOptions());
                listener.addAcceptedListener(listenerHandler::handleConnection);
                listener.start();
            }
        }

        boolean maximumUploadSpeedChanged = patch.getMaximumUploadSpeed() != null
                && patch.getMaximumUploadSpeed() != options.getMaximumUploadSpeed();
        boolean maximumDownloadSpeedChanged = patch.getMaximumDownloadSpeed() != null
                && patch.getMaximumDownloadSpeed() != options.getMaximumDownloadSpeed();
        options = options.with(patch);

        if (maximumUploadSpeedChanged) {
            uploadTokenBucket.setCapacity((options.getMaximumUploadSpeed() * 1024L) / 10);
        }
        if (maximumDownloadSpeedChanged) {
            downloadTokenBucket.setCapacity((options.getMaximumDownloadSpeed() * 1024L) / 10);
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
            // Never held on the failing path, so nothing to release.
            Permits.acquire(stateSemaphore, cancellationSignal);
        } catch (RuntimeException failure) {
            throw reportReconfigureFailure(failure);
        }
        try {
            return performReconfigureOptions(patch, cancellationSignal);
        } catch (Throwable failure) {
            throw reportReconfigureFailure(failure);
        } finally {
            stateSemaphore.release();
        }
    }

    /** Classifies a reconfiguration failure, which is never rolled back. */
    private RuntimeException reportReconfigureFailure(Throwable failure) {
        Throwable cause = Failures.unwrap(failure);
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
        return new SoulseekClientOptionsPatch(
                patch.getEnableListener(),
                patch.getListenIpAddress(),
                patch.getListenPort(),
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
                patch.getIncomingConnectionOptions(),
                null,
                null,
                null);
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

    void cleanupUploadSemaphores() {
        if (!uploadSemaphoreSyncRoot.tryAcquire()) {
            return;
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
        } finally {
            uploadSemaphoreSyncRoot.release();
        }
    }

    // ---- EngineContext, the seam the components delegate through ----------

    /** The periodic endpoint-semaphore sweep; exposed for tests. */
    void cleanupUserEndpointSemaphores() {
        users.cleanupUserEndpointSemaphores();
    }

    @Override
    public java.net.InetSocketAddress resolveUserEndpoint(String username, CancellationSignal cancellationSignal) {
        return users.getUserEndpoint(username, cancellationSignal);
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
        events.raise(Kind.BROWSE_PROGRESS_UPDATED, eventData);
    }

    @Override
    public void acknowledgePrivateMessageOperation(int privateMessageId, CancellationSignal cancellationSignal) {
        server.acknowledgePrivateMessage(privateMessageId, cancellationSignal);
    }

    @Override
    public void acknowledgePrivilegeNotificationOperation(int notificationId, CancellationSignal cancellationSignal) {
        server.acknowledgePrivilegeNotification(notificationId, cancellationSignal);
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
    public <T> void raiseEvent(EngineEvents.Kind kind, T eventData) {
        events.raise(kind, eventData);
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
        return server.username();
    }

    @Override
    public SoulseekClientOptions getClientOptions() {
        return options;
    }

    @Override
    public DiagnosticSink getDiagnostic() {
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

    TransferEngine transfers() {
        return transfers;
    }
}
