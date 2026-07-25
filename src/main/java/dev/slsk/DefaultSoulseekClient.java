// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static dev.slsk.ClientSupport.acquirePermit;
import static dev.slsk.ClientSupport.failureMessage;
import static dev.slsk.ClientSupport.mapClientFailure;
import static dev.slsk.ClientSupport.requireNonEmpty;
import static dev.slsk.ClientSupport.requireText;
import static dev.slsk.ClientSupport.unwrap;

import dev.slsk.common.DefaultWaiter;
import dev.slsk.common.IOAdapter;
import dev.slsk.common.NetworkExecutor;
import dev.slsk.common.Scheduler;
import dev.slsk.common.TokenBucket;
import dev.slsk.common.TokenFactory;
import dev.slsk.common.WaitKey;
import dev.slsk.common.Waiter;
import dev.slsk.diagnostics.DiagnosticEventListener;
import dev.slsk.diagnostics.DiagnosticSink;
import dev.slsk.diagnostics.FilteringDiagnosticSink;
import dev.slsk.diagnostics.GlobalDiagnostic;
import dev.slsk.events.BrowseProgressUpdatedEvent;
import dev.slsk.events.DistributedChildEvent;
import dev.slsk.events.DistributedParentEvent;
import dev.slsk.events.DownloadDeniedEvent;
import dev.slsk.events.DownloadFailedEvent;
import dev.slsk.events.PrivateMessageReceivedEvent;
import dev.slsk.events.PrivilegeNotificationReceivedEvent;
import dev.slsk.events.PublicChatMessageReceivedEvent;
import dev.slsk.events.RoomJoinedEvent;
import dev.slsk.events.RoomLeftEvent;
import dev.slsk.events.RoomMessageReceivedEvent;
import dev.slsk.events.RoomTickerAddedEvent;
import dev.slsk.events.RoomTickerListReceivedEvent;
import dev.slsk.events.RoomTickerRemovedEvent;
import dev.slsk.events.SearchRequestEvent;
import dev.slsk.events.SearchRequestResponseEvent;
import dev.slsk.events.SearchResponseReceivedEvent;
import dev.slsk.events.SearchStateChangedEvent;
import dev.slsk.events.SoulseekClientDisconnectedEvent;
import dev.slsk.events.SoulseekClientStateChangedEvent;
import dev.slsk.events.TransferProgressUpdatedEvent;
import dev.slsk.events.TransferStateChangedEvent;
import dev.slsk.events.UserCannotConnectEvent;
import dev.slsk.exceptions.AddressException;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.ConnectionReadException;
import dev.slsk.exceptions.DuplicateTokenException;
import dev.slsk.exceptions.DuplicateTransferException;
import dev.slsk.exceptions.KickedFromServerException;
import dev.slsk.exceptions.ListenException;
import dev.slsk.exceptions.LoginRejectedException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.TransferException;
import dev.slsk.exceptions.TransferNotFoundException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferReportedFailedException;
import dev.slsk.exceptions.TransferSizeMismatchException;
import dev.slsk.exceptions.TransferStreamException;
import dev.slsk.exceptions.UserEndpointCacheException;
import dev.slsk.exceptions.UserEndpointException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.handlers.DefaultDistributedMessageHandler;
import dev.slsk.messaging.handlers.DefaultPeerMessageHandler;
import dev.slsk.messaging.handlers.DefaultServerMessageHandler;
import dev.slsk.messaging.handlers.DistributedMessageHandler;
import dev.slsk.messaging.handlers.DistributedMessageHandlerClient;
import dev.slsk.messaging.handlers.PeerMessageHandler;
import dev.slsk.messaging.handlers.PeerMessageHandlerClient;
import dev.slsk.messaging.handlers.ServerMessageEvent;
import dev.slsk.messaging.handlers.ServerMessageHandler;
import dev.slsk.messaging.handlers.ServerMessageHandlerClient;
import dev.slsk.messaging.messages.FolderContentsRequest;
import dev.slsk.messaging.messages.LoginRequest;
import dev.slsk.messaging.messages.LoginResponse;
import dev.slsk.messaging.messages.OutgoingMessage;
import dev.slsk.messaging.messages.PlaceInQueueRequest;
import dev.slsk.messaging.messages.PlaceInQueueResponse;
import dev.slsk.messaging.messages.PrivateRoomToggle;
import dev.slsk.messaging.messages.SetListenPortCommand;
import dev.slsk.messaging.messages.TransferRequest;
import dev.slsk.messaging.messages.TransferResponse;
import dev.slsk.messaging.messages.UploadDenied;
import dev.slsk.messaging.messages.UploadFailed;
import dev.slsk.messaging.messages.UserAddressRequest;
import dev.slsk.messaging.messages.UserAddressResponse;
import dev.slsk.network.ConnectionFactory;
import dev.slsk.network.DefaultConnectionFactory;
import dev.slsk.network.DefaultDistributedConnectionManager;
import dev.slsk.network.DefaultListenerHandler;
import dev.slsk.network.DefaultPeerConnectionManager;
import dev.slsk.network.DistributedConnectionManager;
import dev.slsk.network.DistributedConnectionManagerClient;
import dev.slsk.network.ListenerHandler;
import dev.slsk.network.ListenerHandlerClient;
import dev.slsk.network.MessageConnection;
import dev.slsk.network.PeerConnectionManager;
import dev.slsk.network.PeerConnectionManagerClient;
import dev.slsk.network.PeerEndpoint;
import dev.slsk.network.tcp.Connection;
import dev.slsk.network.tcp.ConnectionDataEvent;
import dev.slsk.network.tcp.ConnectionDisconnectedEvent;
import dev.slsk.network.tcp.ConnectionEventListener;
import dev.slsk.network.tcp.Listener;
import dev.slsk.network.tcp.SocketListener;
import dev.slsk.options.BrowseOptions;
import dev.slsk.options.BrowseProgress;
import dev.slsk.options.ConnectionOptions;
import dev.slsk.options.DownloadStreamFactory;
import dev.slsk.options.PositionableInputStream;
import dev.slsk.options.PositionableOutputStream;
import dev.slsk.options.SoulseekClientOptions;
import dev.slsk.options.SoulseekClientOptionsPatch;
import dev.slsk.options.TransferOptions;
import dev.slsk.options.TransferProgressUpdate;
import dev.slsk.options.TransferStateChange;
import dev.slsk.options.UploadStreamFactory;
import dev.slsk.search.DefaultSearchResponder;
import dev.slsk.search.SearchInternal;
import dev.slsk.search.SearchResponder;
import dev.slsk.search.SearchResponderClient;
import dev.slsk.transfer.TransferInternal;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * A client for the Soulseek file-sharing network.
 */
final class DefaultSoulseekClient
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

    private volatile SoulseekClientOptions options;
    private final int minorVersion;
    private final Waiter waiter;
    private final TokenFactory tokenFactory;
    private final Semaphore searchSemaphore;
    private final Semaphore stateSemaphore = new Semaphore(1);
    private final Semaphore globalDownloadSemaphore;
    private final Semaphore globalUploadSemaphore;
    private final Semaphore uploadSemaphoreSyncRoot = new Semaphore(1);
    private final Semaphore userEndpointSemaphoreSyncRoot = new Semaphore(1);
    private final IOAdapter ioAdapter;
    private final TokenBucket uploadTokenBucket;
    private final TokenBucket downloadTokenBucket;
    private final ConnectionFactory connectionFactory;
    private final ListenerHandler listenerHandler;
    private final SearchResponder searchResponder;
    private final PeerMessageHandler peerMessageHandler;
    private final DistributedMessageHandler distributedMessageHandler;
    private final PeerConnectionManager peerConnectionManager;
    private final DistributedConnectionManager distributedConnectionManager;
    private final ServerMessageHandler serverMessageHandler;
    private final DiagnosticSink diagnostic;
    private volatile ClientListenerFactory clientListenerFactory = SocketListener::new;
    private final AtomicBoolean closed = new AtomicBoolean();
    /**
     * The client's single timer thread. Every component that needs delayed or
     * periodic work shares it: the waiter, both token buckets, the distributed
     * status watchdog, semaphore cleanup, and each active search. Before this
     * the client owned four platform threads at rest plus one per search.
     */
    private final Scheduler scheduler;

    /** Chat rooms, split out of this class; see RoomRegistry. */
    private final RoomRegistry rooms;

    /** User info, presence and browsing, split out; see UserDirectory. */
    private final UserDirectory users;

    /** Stateless server commands, split out; see ServerSession. */
    private final ServerSession server;

    /** Caller-facing search lifecycle, split out; see SearchCoordinator. */
    private final SearchCoordinator searchCoordinator;

    private final Map<Event, CopyOnWriteArrayList<SoulseekClientEventListener<?>>> listeners =
            new EnumMap<>(Event.class);

    private volatile MessageConnection serverConnection;
    private volatile Listener listener;
    private volatile String address;
    private volatile InetSocketAddress ipEndpoint;
    private volatile String username;
    private volatile ServerInfo serverInfo = new ServerInfo();
    private volatile SoulseekClientState state = SoulseekClientState.DISCONNECTED;
    private volatile Map<Integer, TransferInternal> downloads = new ConcurrentHashMap<>();
    private volatile Map<Integer, TransferInternal> uploads = new ConcurrentHashMap<>();
    private volatile Map<Integer, SearchInternal> searches = new ConcurrentHashMap<>();
    private final Map<String, Boolean> uniqueKeys = new ConcurrentHashMap<>();
    private final Map<String, Semaphore> userEndpointSemaphores = new ConcurrentHashMap<>();
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
        for (Event event : Event.values()) {
            listeners.put(event, new CopyOnWriteArrayList<>());
        }

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

        scheduler.scheduleAtFixedRate(() -> cleanupUserEndpointSemaphoresAsync(), 5, 5, TimeUnit.MINUTES);
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

    public final void addBrowseProgressUpdatedListener(SoulseekClientEventListener<BrowseProgressUpdatedEvent> value) {
        add(Event.BROWSE_PROGRESS_UPDATED, value);
    }

    public final void removeBrowseProgressUpdatedListener(
            SoulseekClientEventListener<BrowseProgressUpdatedEvent> value) {
        remove(Event.BROWSE_PROGRESS_UPDATED, value);
    }

    public final void addConnectedListener(SoulseekClientEventListener<Void> value) {
        add(Event.CONNECTED, value);
    }

    public final void removeConnectedListener(SoulseekClientEventListener<Void> value) {
        remove(Event.CONNECTED, value);
    }

    public final void addDemotedFromDistributedBranchRootListener(SoulseekClientEventListener<Void> value) {
        add(Event.DEMOTED_FROM_DISTRIBUTED_BRANCH_ROOT, value);
    }

    public final void removeDemotedFromDistributedBranchRootListener(SoulseekClientEventListener<Void> value) {
        remove(Event.DEMOTED_FROM_DISTRIBUTED_BRANCH_ROOT, value);
    }

    public final void addDiagnosticGeneratedListener(DiagnosticEventListener value) {
        add(Event.DIAGNOSTIC_GENERATED, value);
    }

    public final void removeDiagnosticGeneratedListener(DiagnosticEventListener value) {
        remove(Event.DIAGNOSTIC_GENERATED, value);
    }

    public final void addDisconnectedListener(SoulseekClientEventListener<SoulseekClientDisconnectedEvent> value) {
        add(Event.DISCONNECTED, value);
    }

    public final void removeDisconnectedListener(SoulseekClientEventListener<SoulseekClientDisconnectedEvent> value) {
        remove(Event.DISCONNECTED, value);
    }

    public final void addDistributedChildAddedListener(SoulseekClientEventListener<DistributedChildEvent> value) {
        add(Event.DISTRIBUTED_CHILD_ADDED, value);
    }

    public final void removeDistributedChildAddedListener(SoulseekClientEventListener<DistributedChildEvent> value) {
        remove(Event.DISTRIBUTED_CHILD_ADDED, value);
    }

    public final void addDistributedChildDisconnectedListener(
            SoulseekClientEventListener<DistributedChildEvent> value) {
        add(Event.DISTRIBUTED_CHILD_DISCONNECTED, value);
    }

    public final void removeDistributedChildDisconnectedListener(
            SoulseekClientEventListener<DistributedChildEvent> value) {
        remove(Event.DISTRIBUTED_CHILD_DISCONNECTED, value);
    }

    public final void addDistributedNetworkResetListener(SoulseekClientEventListener<Void> value) {
        add(Event.DISTRIBUTED_NETWORK_RESET, value);
    }

    public final void removeDistributedNetworkResetListener(SoulseekClientEventListener<Void> value) {
        remove(Event.DISTRIBUTED_NETWORK_RESET, value);
    }

    public final void addDistributedNetworkStateChangedListener(
            SoulseekClientEventListener<DistributedNetworkInfo> value) {
        add(Event.DISTRIBUTED_NETWORK_STATE_CHANGED, value);
    }

    public final void removeDistributedNetworkStateChangedListener(
            SoulseekClientEventListener<DistributedNetworkInfo> value) {
        remove(Event.DISTRIBUTED_NETWORK_STATE_CHANGED, value);
    }

    public final void addDistributedParentAdoptedListener(SoulseekClientEventListener<DistributedParentEvent> value) {
        add(Event.DISTRIBUTED_PARENT_ADOPTED, value);
    }

    public final void removeDistributedParentAdoptedListener(
            SoulseekClientEventListener<DistributedParentEvent> value) {
        remove(Event.DISTRIBUTED_PARENT_ADOPTED, value);
    }

    public final void addDistributedParentDisconnectedListener(
            SoulseekClientEventListener<DistributedParentEvent> value) {
        add(Event.DISTRIBUTED_PARENT_DISCONNECTED, value);
    }

    public final void removeDistributedParentDisconnectedListener(
            SoulseekClientEventListener<DistributedParentEvent> value) {
        remove(Event.DISTRIBUTED_PARENT_DISCONNECTED, value);
    }

    public final void addDownloadDeniedListener(SoulseekClientEventListener<DownloadDeniedEvent> value) {
        add(Event.DOWNLOAD_DENIED, value);
    }

    public final void removeDownloadDeniedListener(SoulseekClientEventListener<DownloadDeniedEvent> value) {
        remove(Event.DOWNLOAD_DENIED, value);
    }

    public final void addDownloadFailedListener(SoulseekClientEventListener<DownloadFailedEvent> value) {
        add(Event.DOWNLOAD_FAILED, value);
    }

    public final void removeDownloadFailedListener(SoulseekClientEventListener<DownloadFailedEvent> value) {
        remove(Event.DOWNLOAD_FAILED, value);
    }

    public final void addExcludedSearchPhrasesReceivedListener(SoulseekClientEventListener<List<String>> value) {
        add(Event.EXCLUDED_SEARCH_PHRASES_RECEIVED, value);
    }

    public final void removeExcludedSearchPhrasesReceivedListener(SoulseekClientEventListener<List<String>> value) {
        remove(Event.EXCLUDED_SEARCH_PHRASES_RECEIVED, value);
    }

    public final void addGlobalMessageReceivedListener(SoulseekClientEventListener<String> value) {
        add(Event.GLOBAL_MESSAGE_RECEIVED, value);
    }

    public final void removeGlobalMessageReceivedListener(SoulseekClientEventListener<String> value) {
        remove(Event.GLOBAL_MESSAGE_RECEIVED, value);
    }

    public final void addKickedFromServerListener(SoulseekClientEventListener<Void> value) {
        add(Event.KICKED_FROM_SERVER, value);
    }

    public final void removeKickedFromServerListener(SoulseekClientEventListener<Void> value) {
        remove(Event.KICKED_FROM_SERVER, value);
    }

    public final void addLoggedInListener(SoulseekClientEventListener<Void> value) {
        add(Event.LOGGED_IN, value);
    }

    public final void removeLoggedInListener(SoulseekClientEventListener<Void> value) {
        remove(Event.LOGGED_IN, value);
    }

    public final void addPrivateMessageReceivedListener(
            SoulseekClientEventListener<PrivateMessageReceivedEvent> value) {
        add(Event.PRIVATE_MESSAGE_RECEIVED, value);
    }

    public final void removePrivateMessageReceivedListener(
            SoulseekClientEventListener<PrivateMessageReceivedEvent> value) {
        remove(Event.PRIVATE_MESSAGE_RECEIVED, value);
    }

    public final void addPrivateRoomMembershipAddedListener(SoulseekClientEventListener<String> value) {
        add(Event.PRIVATE_ROOM_MEMBERSHIP_ADDED, value);
    }

    public final void removePrivateRoomMembershipAddedListener(SoulseekClientEventListener<String> value) {
        remove(Event.PRIVATE_ROOM_MEMBERSHIP_ADDED, value);
    }

    public final void addPrivateRoomMembershipRemovedListener(SoulseekClientEventListener<String> value) {
        add(Event.PRIVATE_ROOM_MEMBERSHIP_REMOVED, value);
    }

    public final void removePrivateRoomMembershipRemovedListener(SoulseekClientEventListener<String> value) {
        remove(Event.PRIVATE_ROOM_MEMBERSHIP_REMOVED, value);
    }

    public final void addPrivateRoomModeratedUserListReceivedListener(SoulseekClientEventListener<RoomInfo> value) {
        add(Event.PRIVATE_ROOM_MODERATED_USER_LIST_RECEIVED, value);
    }

    public final void removePrivateRoomModeratedUserListReceivedListener(SoulseekClientEventListener<RoomInfo> value) {
        remove(Event.PRIVATE_ROOM_MODERATED_USER_LIST_RECEIVED, value);
    }

    public final void addPrivateRoomModerationAddedListener(SoulseekClientEventListener<String> value) {
        add(Event.PRIVATE_ROOM_MODERATION_ADDED, value);
    }

    public final void removePrivateRoomModerationAddedListener(SoulseekClientEventListener<String> value) {
        remove(Event.PRIVATE_ROOM_MODERATION_ADDED, value);
    }

    public final void addPrivateRoomModerationRemovedListener(SoulseekClientEventListener<String> value) {
        add(Event.PRIVATE_ROOM_MODERATION_REMOVED, value);
    }

    public final void removePrivateRoomModerationRemovedListener(SoulseekClientEventListener<String> value) {
        remove(Event.PRIVATE_ROOM_MODERATION_REMOVED, value);
    }

    public final void addPrivateRoomUserListReceivedListener(SoulseekClientEventListener<RoomInfo> value) {
        add(Event.PRIVATE_ROOM_USER_LIST_RECEIVED, value);
    }

    public final void removePrivateRoomUserListReceivedListener(SoulseekClientEventListener<RoomInfo> value) {
        remove(Event.PRIVATE_ROOM_USER_LIST_RECEIVED, value);
    }

    public final void addPrivilegedUserListReceivedListener(SoulseekClientEventListener<List<String>> value) {
        add(Event.PRIVILEGED_USER_LIST_RECEIVED, value);
    }

    public final void removePrivilegedUserListReceivedListener(SoulseekClientEventListener<List<String>> value) {
        remove(Event.PRIVILEGED_USER_LIST_RECEIVED, value);
    }

    public final void addPrivilegeNotificationReceivedListener(
            SoulseekClientEventListener<PrivilegeNotificationReceivedEvent> value) {
        add(Event.PRIVILEGE_NOTIFICATION_RECEIVED, value);
    }

    public final void removePrivilegeNotificationReceivedListener(
            SoulseekClientEventListener<PrivilegeNotificationReceivedEvent> value) {
        remove(Event.PRIVILEGE_NOTIFICATION_RECEIVED, value);
    }

    public final void addPromotedToDistributedBranchRootListener(SoulseekClientEventListener<Void> value) {
        add(Event.PROMOTED_TO_DISTRIBUTED_BRANCH_ROOT, value);
    }

    public final void removePromotedToDistributedBranchRootListener(SoulseekClientEventListener<Void> value) {
        remove(Event.PROMOTED_TO_DISTRIBUTED_BRANCH_ROOT, value);
    }

    public final void addPublicChatMessageReceivedListener(
            SoulseekClientEventListener<PublicChatMessageReceivedEvent> value) {
        add(Event.PUBLIC_CHAT_MESSAGE_RECEIVED, value);
    }

    public final void removePublicChatMessageReceivedListener(
            SoulseekClientEventListener<PublicChatMessageReceivedEvent> value) {
        remove(Event.PUBLIC_CHAT_MESSAGE_RECEIVED, value);
    }

    public final void addRoomJoinedListener(SoulseekClientEventListener<RoomJoinedEvent> value) {
        add(Event.ROOM_JOINED, value);
    }

    public final void removeRoomJoinedListener(SoulseekClientEventListener<RoomJoinedEvent> value) {
        remove(Event.ROOM_JOINED, value);
    }

    public final void addRoomLeftListener(SoulseekClientEventListener<RoomLeftEvent> value) {
        add(Event.ROOM_LEFT, value);
    }

    public final void removeRoomLeftListener(SoulseekClientEventListener<RoomLeftEvent> value) {
        remove(Event.ROOM_LEFT, value);
    }

    public final void addRoomListReceivedListener(SoulseekClientEventListener<RoomList> value) {
        add(Event.ROOM_LIST_RECEIVED, value);
    }

    public final void removeRoomListReceivedListener(SoulseekClientEventListener<RoomList> value) {
        remove(Event.ROOM_LIST_RECEIVED, value);
    }

    public final void addRoomMessageReceivedListener(SoulseekClientEventListener<RoomMessageReceivedEvent> value) {
        add(Event.ROOM_MESSAGE_RECEIVED, value);
    }

    public final void removeRoomMessageReceivedListener(SoulseekClientEventListener<RoomMessageReceivedEvent> value) {
        remove(Event.ROOM_MESSAGE_RECEIVED, value);
    }

    public final void addRoomTickerAddedListener(SoulseekClientEventListener<RoomTickerAddedEvent> value) {
        add(Event.ROOM_TICKER_ADDED, value);
    }

    public final void removeRoomTickerAddedListener(SoulseekClientEventListener<RoomTickerAddedEvent> value) {
        remove(Event.ROOM_TICKER_ADDED, value);
    }

    public final void addRoomTickerListReceivedListener(
            SoulseekClientEventListener<RoomTickerListReceivedEvent> value) {
        add(Event.ROOM_TICKER_LIST_RECEIVED, value);
    }

    public final void removeRoomTickerListReceivedListener(
            SoulseekClientEventListener<RoomTickerListReceivedEvent> value) {
        remove(Event.ROOM_TICKER_LIST_RECEIVED, value);
    }

    public final void addRoomTickerRemovedListener(SoulseekClientEventListener<RoomTickerRemovedEvent> value) {
        add(Event.ROOM_TICKER_REMOVED, value);
    }

    public final void removeRoomTickerRemovedListener(SoulseekClientEventListener<RoomTickerRemovedEvent> value) {
        remove(Event.ROOM_TICKER_REMOVED, value);
    }

    public final void addSearchRequestReceivedListener(SoulseekClientEventListener<SearchRequestEvent> value) {
        add(Event.SEARCH_REQUEST_RECEIVED, value);
    }

    public final void removeSearchRequestReceivedListener(SoulseekClientEventListener<SearchRequestEvent> value) {
        remove(Event.SEARCH_REQUEST_RECEIVED, value);
    }

    public final void addSearchResponseDeliveredListener(
            SoulseekClientEventListener<SearchRequestResponseEvent> value) {
        add(Event.SEARCH_RESPONSE_DELIVERED, value);
    }

    public final void removeSearchResponseDeliveredListener(
            SoulseekClientEventListener<SearchRequestResponseEvent> value) {
        remove(Event.SEARCH_RESPONSE_DELIVERED, value);
    }

    public final void addSearchResponseDeliveryFailedListener(
            SoulseekClientEventListener<SearchRequestResponseEvent> value) {
        add(Event.SEARCH_RESPONSE_DELIVERY_FAILED, value);
    }

    public final void removeSearchResponseDeliveryFailedListener(
            SoulseekClientEventListener<SearchRequestResponseEvent> value) {
        remove(Event.SEARCH_RESPONSE_DELIVERY_FAILED, value);
    }

    public final void addSearchResponseReceivedListener(
            SoulseekClientEventListener<SearchResponseReceivedEvent> value) {
        add(Event.SEARCH_RESPONSE_RECEIVED, value);
    }

    public final void removeSearchResponseReceivedListener(
            SoulseekClientEventListener<SearchResponseReceivedEvent> value) {
        remove(Event.SEARCH_RESPONSE_RECEIVED, value);
    }

    public final void addSearchStateChangedListener(SoulseekClientEventListener<SearchStateChangedEvent> value) {
        add(Event.SEARCH_STATE_CHANGED, value);
    }

    public final void removeSearchStateChangedListener(SoulseekClientEventListener<SearchStateChangedEvent> value) {
        remove(Event.SEARCH_STATE_CHANGED, value);
    }

    public final void addServerInfoReceivedListener(SoulseekClientEventListener<ServerInfo> value) {
        add(Event.SERVER_INFO_RECEIVED, value);
    }

    public final void removeServerInfoReceivedListener(SoulseekClientEventListener<ServerInfo> value) {
        remove(Event.SERVER_INFO_RECEIVED, value);
    }

    public final void addStateChangedListener(SoulseekClientEventListener<SoulseekClientStateChangedEvent> value) {
        add(Event.STATE_CHANGED, value);
    }

    public final void removeStateChangedListener(SoulseekClientEventListener<SoulseekClientStateChangedEvent> value) {
        remove(Event.STATE_CHANGED, value);
    }

    public final void addTransferProgressUpdatedListener(
            SoulseekClientEventListener<TransferProgressUpdatedEvent> value) {
        add(Event.TRANSFER_PROGRESS_UPDATED, value);
    }

    public final void removeTransferProgressUpdatedListener(
            SoulseekClientEventListener<TransferProgressUpdatedEvent> value) {
        remove(Event.TRANSFER_PROGRESS_UPDATED, value);
    }

    public final void addTransferStateChangedListener(SoulseekClientEventListener<TransferStateChangedEvent> value) {
        add(Event.TRANSFER_STATE_CHANGED, value);
    }

    public final void removeTransferStateChangedListener(SoulseekClientEventListener<TransferStateChangedEvent> value) {
        remove(Event.TRANSFER_STATE_CHANGED, value);
    }

    public final void addUserCannotConnectListener(SoulseekClientEventListener<UserCannotConnectEvent> value) {
        add(Event.USER_CANNOT_CONNECT, value);
    }

    public final void removeUserCannotConnectListener(SoulseekClientEventListener<UserCannotConnectEvent> value) {
        remove(Event.USER_CANNOT_CONNECT, value);
    }

    public final void addUserStatisticsChangedListener(SoulseekClientEventListener<UserStatistics> value) {
        add(Event.USER_STATISTICS_CHANGED, value);
    }

    public final void removeUserStatisticsChangedListener(SoulseekClientEventListener<UserStatistics> value) {
        remove(Event.USER_STATISTICS_CHANGED, value);
    }

    public final void addUserStatusChangedListener(SoulseekClientEventListener<UserStatus> value) {
        add(Event.USER_STATUS_CHANGED, value);
    }

    public final void removeUserStatusChangedListener(SoulseekClientEventListener<UserStatus> value) {
        remove(Event.USER_STATUS_CHANGED, value);
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

    private CompletableFuture<List<Directory>> getDirectoryContentsOperation(
            String requestedUsername, String directoryName) {
        return getDirectoryContentsOperation(requestedUsername, directoryName, null, CancellationSignal.none());
    }

    private CompletableFuture<List<Directory>> getDirectoryContentsOperation(
            String requestedUsername, String directoryName, int operationToken) {
        return getDirectoryContentsOperation(
                requestedUsername, directoryName, operationToken, CancellationSignal.none());
    }

    private CompletableFuture<List<Directory>> getDirectoryContentsOperation(
            String requestedUsername, String directoryName, CancellationSignal cancellationSignal) {
        return getDirectoryContentsOperation(requestedUsername, directoryName, null, cancellationSignal);
    }

    private CompletableFuture<List<Directory>> getDirectoryContentsOperation(
            String requestedUsername,
            String directoryName,
            Integer operationToken,
            CancellationSignal cancellationSignal) {
        requireText(requestedUsername, "username");
        requireText(directoryName, "directoryName");
        requireLoggedIn("fetch directory contents");
        int tokenValue = operationToken == null ? getNextToken() : operationToken;
        CancellationSignal token = defaultToken(cancellationSignal);
        CompletableFuture<List<Directory>> contentsWait;
        try {
            @SuppressWarnings("unchecked")
            CompletableFuture<List<Directory>> typedWait =
                    (CompletableFuture<List<Directory>>) (CompletableFuture<?>) waiter.waitAsync(
                            new WaitKey(MessageCode.Peer.FOLDER_CONTENTS_RESPONSE, requestedUsername, tokenValue),
                            List.class,
                            null,
                            token);
            contentsWait = typedWait;
        } catch (Throwable failure) {
            return mapClientFailure(
                    CompletableFuture.failedFuture(failure),
                    "Failed to retrieve directory contents for " + directoryName + " from " + requestedUsername + ": ",
                    UserOfflineException.class);
        }
        CompletableFuture<List<Directory>> operation = getUserEndpointOperation(requestedUsername, token)
                .thenCompose(endpoint ->
                        peerConnectionManager.getOrAddMessageConnectionAsync(requestedUsername, endpoint, token))
                .thenCompose(connection ->
                        invokeMessageWrite(connection, new FolderContentsRequest(tokenValue, directoryName), token))
                .thenCompose(ignored -> contentsWait)
                .thenApply(response -> Collections.unmodifiableList(new ArrayList<>(response)));
        return mapClientFailure(
                operation,
                "Failed to retrieve directory contents for " + directoryName + " from " + requestedUsername + ": ",
                UserOfflineException.class);
    }

    private CompletableFuture<Integer> getDownloadPlaceInQueueOperation(String requestedUsername, String filename) {
        return getDownloadPlaceInQueueOperation(requestedUsername, filename, CancellationSignal.none());
    }

    private CompletableFuture<Integer> getDownloadPlaceInQueueOperation(
            String requestedUsername, String filename, CancellationSignal cancellationSignal) {
        requireText(requestedUsername, "username");
        requireText(filename, "filename");
        requireLoggedIn("check download queue position");
        boolean active = downloads.values().stream()
                .anyMatch(download -> Objects.equals(download.getUsername(), requestedUsername)
                        && Objects.equals(download.getFilename(), filename));
        if (!active) {
            throw new TransferNotFoundException(
                    "A download of " + filename + " from user " + requestedUsername + " is not active");
        }
        CancellationSignal token = defaultToken(cancellationSignal);
        CompletableFuture<PlaceInQueueResponse> responseWait;
        try {
            responseWait = waiter.waitAsync(
                    new WaitKey(MessageCode.Peer.PLACE_IN_QUEUE_RESPONSE, requestedUsername, filename),
                    PlaceInQueueResponse.class,
                    null,
                    token);
        } catch (Throwable failure) {
            return mapClientFailure(
                    CompletableFuture.failedFuture(failure),
                    "Failed to fetch place in queue for download of " + filename + " from " + requestedUsername + ": ",
                    UserOfflineException.class);
        }
        CompletableFuture<Integer> operation = getUserEndpointOperation(requestedUsername, token)
                .thenCompose(endpoint ->
                        peerConnectionManager.getOrAddMessageConnectionAsync(requestedUsername, endpoint, token))
                .thenCompose(connection -> invokeMessageWrite(connection, new PlaceInQueueRequest(filename), token))
                .thenCompose(ignored -> responseWait)
                .thenApply(PlaceInQueueResponse::getPlaceInQueue);
        return mapClientFailure(
                operation,
                "Failed to fetch place in queue for download of " + filename + " from " + requestedUsername + ": ",
                UserOfflineException.class);
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
        return reconfigureOptionsInternalAsync(patch, defaultToken(cancellationSignal));
    }

    /** Downloads a remote file to a local file. */
    private CompletableFuture<Transfer> downloadOperation(
            String requestedUsername, String remoteFilename, String localFilename) {
        return downloadOperation(
                requestedUsername, remoteFilename, localFilename, null, 0, null, null, CancellationSignal.none());
    }

    /** Downloads a remote file with an expected size. */
    private CompletableFuture<Transfer> downloadOperation(
            String requestedUsername, String remoteFilename, String localFilename, Long size) {
        return downloadOperation(
                requestedUsername, remoteFilename, localFilename, size, 0, null, null, CancellationSignal.none());
    }

    /** Downloads a remote file with cancellation. */
    private CompletableFuture<Transfer> downloadOperation(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            CancellationSignal cancellationSignal) {
        return downloadOperation(
                requestedUsername, remoteFilename, localFilename, null, 0, null, null, cancellationSignal);
    }

    /** Downloads a remote file from a resume offset. */
    private CompletableFuture<Transfer> downloadOperation(
            String requestedUsername, String remoteFilename, String localFilename, Long size, long startOffset) {
        return downloadOperation(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                null,
                null,
                CancellationSignal.none());
    }

    /** Downloads a remote file with a specific token. */
    private CompletableFuture<Transfer> downloadOperation(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token) {
        return downloadOperation(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                token,
                null,
                CancellationSignal.none());
    }

    /** Downloads a remote file using supplied transfer options. */
    private CompletableFuture<Transfer> downloadOperation(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions) {
        return downloadOperation(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                token,
                transferOptions,
                CancellationSignal.none());
    }

    /** Downloads a remote file to a local file. */
    private CompletableFuture<Transfer> downloadOperation(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        requireText(requestedUsername, "username");
        requireText(remoteFilename, "remoteFilename");
        requireText(localFilename, "localFilename");
        validateDownloadRange(size, startOffset);
        requireLoggedIn("download files");
        int transferToken = token == null ? getNextToken() : token;
        validateDownloadUniqueness(requestedUsername, remoteFilename, transferToken);
        TransferOptions options =
                (transferOptions == null ? new TransferOptions() : transferOptions).withDisposalOptions(null, true);
        return downloadToStreamAsync(
                requestedUsername,
                remoteFilename,
                () -> {
                    try {
                        return CompletableFuture.completedFuture(
                                ioAdapter.getOutputStream(localFilename, startOffset > 0));
                    } catch (IOException failure) {
                        return CompletableFuture.failedFuture(new UncheckedIOException(failure));
                    }
                },
                size,
                startOffset,
                transferToken,
                options,
                defaultToken(cancellationSignal));
    }

    /** Downloads data to a stream created by a factory. */
    private CompletableFuture<Transfer> downloadOperation(
            String requestedUsername, String remoteFilename, DownloadStreamFactory outputStreamFactory) {
        return downloadOperation(
                requestedUsername, remoteFilename, outputStreamFactory, null, 0, null, null, CancellationSignal.none());
    }

    /** Downloads stream data with an expected size. */
    private CompletableFuture<Transfer> downloadOperation(
            String requestedUsername, String remoteFilename, DownloadStreamFactory outputStreamFactory, Long size) {
        return downloadOperation(
                requestedUsername, remoteFilename, outputStreamFactory, size, 0, null, null, CancellationSignal.none());
    }

    /** Downloads stream data with cancellation. */
    private CompletableFuture<Transfer> downloadOperation(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            CancellationSignal cancellationSignal) {
        return downloadOperation(
                requestedUsername, remoteFilename, outputStreamFactory, null, 0, null, null, cancellationSignal);
    }

    /** Downloads stream data from a resume offset. */
    private CompletableFuture<Transfer> downloadOperation(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset) {
        return downloadOperation(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                null,
                null,
                CancellationSignal.none());
    }

    /** Downloads stream data with a specific token. */
    private CompletableFuture<Transfer> downloadOperation(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token) {
        return downloadOperation(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                token,
                null,
                CancellationSignal.none());
    }

    /** Downloads stream data using supplied transfer options. */
    private CompletableFuture<Transfer> downloadOperation(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions) {
        return downloadOperation(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                token,
                transferOptions,
                CancellationSignal.none());
    }

    /** Downloads data to a stream created by a factory. */
    private CompletableFuture<Transfer> downloadOperation(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        requireText(requestedUsername, "username");
        requireText(remoteFilename, "remoteFilename");
        validateDownloadRange(size, startOffset);
        Objects.requireNonNull(outputStreamFactory, "outputStreamFactory");
        requireLoggedIn("download files");
        int transferToken = token == null ? getNextToken() : token;
        validateDownloadUniqueness(requestedUsername, remoteFilename, transferToken);
        return downloadToStreamAsync(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                transferToken,
                transferOptions == null ? new TransferOptions() : transferOptions,
                defaultToken(cancellationSignal));
    }

    /** Enqueues a local-file download. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadOperation(
            String requestedUsername, String remoteFilename, String localFilename) {
        return enqueueDownloadOperation(
                requestedUsername, remoteFilename, localFilename, null, 0, null, null, CancellationSignal.none());
    }

    /** Enqueues a local-file download with an expected size. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadOperation(
            String requestedUsername, String remoteFilename, String localFilename, Long size) {
        return enqueueDownloadOperation(
                requestedUsername, remoteFilename, localFilename, size, 0, null, null, CancellationSignal.none());
    }

    /** Enqueues a local-file download from a resume offset. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadOperation(
            String requestedUsername, String remoteFilename, String localFilename, Long size, long startOffset) {
        return enqueueDownloadOperation(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                null,
                null,
                CancellationSignal.none());
    }

    /** Enqueues a local-file download with a specific token. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadOperation(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token) {
        return enqueueDownloadOperation(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                token,
                null,
                CancellationSignal.none());
    }

    /** Enqueues a local-file download using supplied options. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadOperation(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions) {
        return enqueueDownloadOperation(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                token,
                transferOptions,
                CancellationSignal.none());
    }

    /** Enqueues a local-file download. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadOperation(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        CompletableFuture<Boolean> enqueued = new CompletableFuture<>();
        TransferOptions options = (transferOptions == null ? new TransferOptions() : transferOptions)
                .withAdditionalStateChanged(change ->
                        completeDownloadEnqueue(enqueued, change.transfer().getState()));
        CompletableFuture<Transfer> download = downloadOperation(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                token,
                options,
                cancellationSignal);
        return nestedDownloadWhenEnqueued(enqueued, download);
    }

    /** Enqueues a stream-factory download. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadOperation(
            String requestedUsername, String remoteFilename, DownloadStreamFactory outputStreamFactory) {
        return enqueueDownloadOperation(
                requestedUsername, remoteFilename, outputStreamFactory, null, 0, null, null, CancellationSignal.none());
    }

    /** Enqueues a stream-factory download with an expected size. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadOperation(
            String requestedUsername, String remoteFilename, DownloadStreamFactory outputStreamFactory, Long size) {
        return enqueueDownloadOperation(
                requestedUsername, remoteFilename, outputStreamFactory, size, 0, null, null, CancellationSignal.none());
    }

    /** Enqueues a stream-factory download from a resume offset. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadOperation(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset) {
        return enqueueDownloadOperation(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                null,
                null,
                CancellationSignal.none());
    }

    /** Enqueues a stream-factory download with a specific token. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadOperation(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token) {
        return enqueueDownloadOperation(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                token,
                null,
                CancellationSignal.none());
    }

    /** Enqueues a stream-factory download using supplied options. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadOperation(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions) {
        return enqueueDownloadOperation(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                token,
                transferOptions,
                CancellationSignal.none());
    }

    /** Enqueues a stream-factory download. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadOperation(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        CompletableFuture<Boolean> enqueued = new CompletableFuture<>();
        TransferOptions options = (transferOptions == null ? new TransferOptions() : transferOptions)
                .withAdditionalStateChanged(change ->
                        completeDownloadEnqueue(enqueued, change.transfer().getState()));
        CompletableFuture<Transfer> download = downloadOperation(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                token,
                options,
                cancellationSignal);
        return nestedDownloadWhenEnqueued(enqueued, download);
    }

    /** Uploads a local file to a peer. */
    private CompletableFuture<Transfer> uploadOperation(
            String requestedUsername, String remoteFilename, String localFilename) {
        return uploadOperation(requestedUsername, remoteFilename, localFilename, null, null, CancellationSignal.none());
    }

    /** Uploads a local file to a peer with a specific token. */
    private CompletableFuture<Transfer> uploadOperation(
            String requestedUsername, String remoteFilename, String localFilename, Integer token) {
        return uploadOperation(
                requestedUsername, remoteFilename, localFilename, token, null, CancellationSignal.none());
    }

    /** Uploads a local file with cancellation. */
    private CompletableFuture<Transfer> uploadOperation(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            CancellationSignal cancellationSignal) {
        return uploadOperation(requestedUsername, remoteFilename, localFilename, null, null, cancellationSignal);
    }

    /** Uploads a local file using the supplied options. */
    private CompletableFuture<Transfer> uploadOperation(
            String requestedUsername, String remoteFilename, String localFilename, TransferOptions transferOptions) {
        return uploadOperation(
                requestedUsername, remoteFilename, localFilename, null, transferOptions, CancellationSignal.none());
    }

    /** Uploads a local file to a peer using the supplied options. */
    private CompletableFuture<Transfer> uploadOperation(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Integer token,
            TransferOptions transferOptions) {
        return uploadOperation(
                requestedUsername, remoteFilename, localFilename, token, transferOptions, CancellationSignal.none());
    }

    /** Uploads a local file to a peer. */
    private CompletableFuture<Transfer> uploadOperation(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        requireText(requestedUsername, "username");
        requireText(remoteFilename, "remoteFilename");
        requireText(localFilename, "localFilename");
        if (!ioAdapter.exists(localFilename)) {
            throw new UncheckedIOException(
                    new FileNotFoundException("The local file does not exist: " + localFilename));
        }
        requireLoggedIn("upload files");
        try (InputStream ignored = ioAdapter.getInputStream(localFilename)) {
            // Probe readability before allocating a transfer token.
        } catch (IOException failure) {
            throw new UncheckedIOException(
                    "The local file " + localFilename + " could not be opened for reading: " + failureMessage(failure),
                    failure);
        }

        int transferToken = token == null ? getNextToken() : token;
        validateUploadUniqueness(requestedUsername, remoteFilename, transferToken);
        TransferOptions options = transferOptions == null ? new TransferOptions() : transferOptions;
        TransferOptions fileOptions = options.withDisposalOptions(true, null);
        long size;
        try {
            size = ioAdapter.getFileInfo(localFilename).size();
        } catch (IOException failure) {
            return CompletableFuture.failedFuture(new UncheckedIOException(failure));
        }
        return uploadFromStreamAsync(
                requestedUsername,
                remoteFilename,
                size,
                ignoredOffset -> {
                    try {
                        return CompletableFuture.completedFuture(ioAdapter.getInputStream(localFilename));
                    } catch (IOException failure) {
                        return CompletableFuture.failedFuture(new UncheckedIOException(failure));
                    }
                },
                transferToken,
                fileOptions,
                defaultToken(cancellationSignal));
    }

    /** Uploads data supplied by an asynchronous stream factory. */
    private CompletableFuture<Transfer> uploadOperation(
            String requestedUsername, String remoteFilename, long size, UploadStreamFactory inputStreamFactory) {
        return uploadOperation(
                requestedUsername, remoteFilename, size, inputStreamFactory, null, null, CancellationSignal.none());
    }

    /** Uploads stream data with a specific transfer token. */
    private CompletableFuture<Transfer> uploadOperation(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token) {
        return uploadOperation(
                requestedUsername, remoteFilename, size, inputStreamFactory, token, null, CancellationSignal.none());
    }

    /** Uploads stream data with cancellation. */
    private CompletableFuture<Transfer> uploadOperation(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            CancellationSignal cancellationSignal) {
        return uploadOperation(
                requestedUsername, remoteFilename, size, inputStreamFactory, null, null, cancellationSignal);
    }

    /** Uploads stream data using the supplied options. */
    private CompletableFuture<Transfer> uploadOperation(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            TransferOptions transferOptions) {
        return uploadOperation(
                requestedUsername,
                remoteFilename,
                size,
                inputStreamFactory,
                null,
                transferOptions,
                CancellationSignal.none());
    }

    /** Uploads stream data using the supplied transfer options. */
    private CompletableFuture<Transfer> uploadOperation(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token,
            TransferOptions transferOptions) {
        return uploadOperation(
                requestedUsername,
                remoteFilename,
                size,
                inputStreamFactory,
                token,
                transferOptions,
                CancellationSignal.none());
    }

    /** Uploads data supplied by an asynchronous stream factory. */
    private CompletableFuture<Transfer> uploadOperation(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        requireText(requestedUsername, "username");
        requireText(remoteFilename, "remoteFilename");
        if (size < 0) {
            throw new IllegalArgumentException("size must be greater than or equal to zero");
        }
        Objects.requireNonNull(inputStreamFactory, "inputStreamFactory");
        requireLoggedIn("upload files");
        int transferToken = token == null ? getNextToken() : token;
        validateUploadUniqueness(requestedUsername, remoteFilename, transferToken);
        return uploadFromStreamAsync(
                requestedUsername,
                remoteFilename,
                size,
                inputStreamFactory,
                transferToken,
                transferOptions == null ? new TransferOptions() : transferOptions,
                defaultToken(cancellationSignal));
    }

    /** Enqueues a local-file upload and returns its nested completion future. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueUploadOperation(
            String requestedUsername, String remoteFilename, String localFilename) {
        return enqueueUploadOperation(
                requestedUsername, remoteFilename, localFilename, null, null, CancellationSignal.none());
    }

    /** Enqueues a local-file upload with a specific token. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueUploadOperation(
            String requestedUsername, String remoteFilename, String localFilename, Integer token) {
        return enqueueUploadOperation(
                requestedUsername, remoteFilename, localFilename, token, null, CancellationSignal.none());
    }

    /** Enqueues a local-file upload with cancellation. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueUploadOperation(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            CancellationSignal cancellationSignal) {
        return enqueueUploadOperation(requestedUsername, remoteFilename, localFilename, null, null, cancellationSignal);
    }

    /** Enqueues a local-file upload using supplied options. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueUploadOperation(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Integer token,
            TransferOptions transferOptions) {
        return enqueueUploadOperation(
                requestedUsername, remoteFilename, localFilename, token, transferOptions, CancellationSignal.none());
    }

    /** Enqueues a local-file upload. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueUploadOperation(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        CompletableFuture<Boolean> enqueued = new CompletableFuture<>();
        TransferOptions options = (transferOptions == null ? new TransferOptions() : transferOptions)
                .withAdditionalStateChanged(change -> {
                    if (change.transfer().getState().equals(TransferState.QUEUED.or(TransferState.LOCALLY))) {
                        enqueued.complete(true);
                    }
                });
        CompletableFuture<Transfer> upload =
                uploadOperation(requestedUsername, remoteFilename, localFilename, token, options, cancellationSignal);
        return enqueued.thenApply(ignored -> upload);
    }

    /** Enqueues a stream-factory upload. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueUploadOperation(
            String requestedUsername, String remoteFilename, long size, UploadStreamFactory inputStreamFactory) {
        return enqueueUploadOperation(
                requestedUsername, remoteFilename, size, inputStreamFactory, null, null, CancellationSignal.none());
    }

    /** Enqueues a stream-factory upload with a specific token. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueUploadOperation(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token) {
        return enqueueUploadOperation(
                requestedUsername, remoteFilename, size, inputStreamFactory, token, null, CancellationSignal.none());
    }

    /** Enqueues a stream-factory upload with cancellation. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueUploadOperation(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            CancellationSignal cancellationSignal) {
        return enqueueUploadOperation(
                requestedUsername, remoteFilename, size, inputStreamFactory, null, null, cancellationSignal);
    }

    /** Enqueues a stream-factory upload using supplied options. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueUploadOperation(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token,
            TransferOptions transferOptions) {
        return enqueueUploadOperation(
                requestedUsername,
                remoteFilename,
                size,
                inputStreamFactory,
                token,
                transferOptions,
                CancellationSignal.none());
    }

    /** Enqueues a stream-factory upload. */
    private CompletableFuture<CompletableFuture<Transfer>> enqueueUploadOperation(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        CompletableFuture<Boolean> enqueued = new CompletableFuture<>();
        TransferOptions options = (transferOptions == null ? new TransferOptions() : transferOptions)
                .withAdditionalStateChanged(change -> {
                    if (change.transfer().getState().equals(TransferState.QUEUED.or(TransferState.LOCALLY))) {
                        enqueued.complete(true);
                    }
                });
        CompletableFuture<Transfer> upload = uploadOperation(
                requestedUsername, remoteFilename, size, inputStreamFactory, token, options, cancellationSignal);
        return enqueued.thenApply(ignored -> upload);
    }

    public CompletableFuture<InetSocketAddress> getUserEndpointOperation(String requestedUsername) {
        return getUserEndpointOperation(requestedUsername, CancellationSignal.none());
    }

    public CompletableFuture<InetSocketAddress> getUserEndpointOperation(
            String requestedUsername, CancellationSignal cancellationSignal) {
        requireText(requestedUsername, "username");
        requireLoggedIn("fetch user endpoint");
        CancellationSignal token = defaultToken(cancellationSignal);
        UserEndpointCache cache = options.getUserEndpointCache();
        if (cache == null) {
            return retrieveUserEndpoint(requestedUsername, token, null);
        }

        CacheLookupResult<InetSocketAddress> cached = tryCacheGet(cache, requestedUsername);
        if (cached.found()) {
            diagnostic.debug("Endpoint cache HIT for " + requestedUsername + ": " + cached.value());
            return CompletableFuture.completedFuture(cached.value());
        }

        // The source serializes same-user lookups only when a cache is configured, so the first
        // caller populates it and the rest read it back. Each caller still issues its own request
        // under its own cancellation signal; sharing one in-flight request would let one caller's
        // cancellation or failure surface in another's.
        Semaphore semaphore;
        userEndpointSemaphoreSyncRoot.acquireUninterruptibly();
        try {
            semaphore = userEndpointSemaphores.computeIfAbsent(requestedUsername, ignored -> new Semaphore(1));
        } finally {
            userEndpointSemaphoreSyncRoot.release();
        }

        // The permit is released only on the path that acquired it; a cancelled acquisition must
        // not release a permit it never held.
        return acquirePermit(semaphore, token).thenCompose(ignored -> {
            CompletableFuture<InetSocketAddress> operation;
            try {
                CacheLookupResult<InetSocketAddress> second = tryCacheGet(cache, requestedUsername);
                if (second.found()) {
                    diagnostic.debug("Endpoint cache HIT for " + requestedUsername + ": " + second.value());
                    operation = CompletableFuture.completedFuture(second.value());
                } else {
                    operation = retrieveUserEndpoint(requestedUsername, token, cache);
                }
            } catch (Throwable failure) {
                semaphore.release();
                throw failure;
            }
            return operation.whenComplete((result, failure) -> semaphore.release());
        });
    }

    /**
     * Removes idle per-user endpoint semaphores.
     *
     * <p>Mirrors the source's periodic cleanup: a semaphore whose permit can be taken has no waiter, so it can be
     * dropped. The sync root is only taken opportunistically, matching the source's zero-timeout wait.
     *
     * @return a future completed when the sweep finishes
     */
    CompletableFuture<Void> cleanupUserEndpointSemaphoresAsync() {
        if (!userEndpointSemaphoreSyncRoot.tryAcquire()) {
            return CompletableFuture.completedFuture(null);
        }
        try {
            for (Map.Entry<String, Semaphore> entry : userEndpointSemaphores.entrySet()) {
                Semaphore semaphore = entry.getValue();
                if (!semaphore.tryAcquire()) {
                    continue;
                }
                if (userEndpointSemaphores.remove(entry.getKey(), semaphore)) {
                    diagnostic.debug("Cleaned up user endpoint semaphore for " + entry.getKey());
                } else {
                    semaphore.release();
                }
            }
            return CompletableFuture.completedFuture(null);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        } finally {
            userEndpointSemaphoreSyncRoot.release();
        }
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

    final IOAdapter getIoAdapter() {
        return ioAdapter;
    }

    final TokenBucket getUploadTokenBucket() {
        return uploadTokenBucket;
    }

    final TokenBucket getDownloadTokenBucket() {
        return downloadTokenBucket;
    }

    final Map<Integer, TransferInternal> getUploadsInternal() {
        return uploads;
    }

    final Map<String, Boolean> getUniqueKeys() {
        return uniqueKeys;
    }

    final Map<String, Semaphore> getUploadSemaphoresForTest() {
        return uploadSemaphores;
    }

    final Map<String, Semaphore> getUserEndpointSemaphoresForTest() {
        return userEndpointSemaphores;
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

    private <T> void add(Event event, SoulseekClientEventListener<T> listener) {
        listeners.get(event).add(Objects.requireNonNull(listener, "listener"));
    }

    private <T> void remove(Event event, SoulseekClientEventListener<T> listener) {
        listeners.get(event).remove(listener);
    }

    private <T> void raise(Event event, T eventData) {
        raiseFrom(this, event, eventData);
    }

    @SuppressWarnings("unchecked")
    private <T> void raiseFrom(Object sender, Event event, T eventData) {
        for (SoulseekClientEventListener<?> listener : listeners.get(event)) {
            ((SoulseekClientEventListener<T>) listener).handle(sender, eventData);
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

    private CompletableFuture<Void> sendConfigurationMessagesAsync(CancellationSignal cancellationSignal) {
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

    private CompletableFuture<Boolean> reconfigureOptionsInternalAsync(
            SoulseekClientOptionsPatch patch, CancellationSignal cancellationSignal) {
        CompletableFuture<Boolean> serialized = acquirePermit(stateSemaphore, cancellationSignal)
                .thenCompose(ignored -> {
                    CompletableFuture<Boolean> operation;
                    try {
                        operation = performReconfigureOptionsAsync(patch, cancellationSignal);
                    } catch (Throwable failure) {
                        operation = CompletableFuture.failedFuture(failure);
                    }
                    return operation.whenComplete((result, failure) -> stateSemaphore.release());
                });
        return serialized.handle((result, failure) -> {
            if (failure == null) {
                return result;
            }
            Throwable cause = unwrap(failure);
            if (cause instanceof CancellationException || cause instanceof TimeoutException) {
                throw new CompletionException(cause);
            }
            throw new CompletionException(new SoulseekClientException(
                    "Failed to reconfigure options: "
                            + failureMessage(cause)
                            + ".  Any successful reconfiguration has not "
                            + "been rolled back; retry with the same patch "
                            + "until successful or consider this as a "
                            + "fatal Exception",
                    cause));
        });
    }

    private CompletableFuture<Boolean> performReconfigureOptionsAsync(
            SoulseekClientOptionsPatch patch, CancellationSignal cancellationSignal) {
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
            return CompletableFuture.completedFuture(false);
        }
        diagnostic.debug("Updating server with latest configuration");
        boolean requiresReconnect = reconnectRequired;
        return sendConfigurationMessagesAsync(cancellationSignal).thenApply(ignored -> {
            if (requiresReconnect) {
                diagnostic.warning("Server reconnect required for options " + "to fully take effect");
            }
            return requiresReconnect;
        });
    }

    private boolean isConnectedAndLoggedIn() {
        return state.contains(SoulseekClientState.CONNECTED) && state.contains(SoulseekClientState.LOGGED_IN);
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
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private static Exception asException(Throwable failure) {
        if (failure instanceof Exception exception) {
            return exception;
        }
        return new RuntimeException(failure);
    }

    private static void validateDownloadRange(Long size, long startOffset) {
        if (size != null && size < 0) {
            throw new IllegalArgumentException("size must be greater than or equal to zero");
        }
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset must be greater than or equal to zero");
        }
        if (startOffset > 0 && size == null) {
            throw new NullPointerException("size must be specified when startOffset is non-zero");
        }
    }

    private void validateDownloadUniqueness(String requestedUsername, String remoteFilename, int token) {
        if (uploads.containsKey(token) || downloads.containsKey(token)) {
            throw new DuplicateTokenException("The specified or generated token " + token + " is already in progress");
        }
        boolean duplicate = downloads.values().stream()
                .anyMatch(download -> Objects.equals(download.getUsername(), requestedUsername)
                        && Objects.equals(download.getFilename(), remoteFilename));
        if (duplicate || uniqueKeys.containsKey(downloadUniqueKey(requestedUsername, remoteFilename))) {
            throw new DuplicateTransferException("An active or queued download of "
                    + remoteFilename + " from " + requestedUsername
                    + " is already in progress");
        }
    }

    private static String downloadUniqueKey(String requestedUsername, String remoteFilename) {
        return "Download:" + requestedUsername + ":" + remoteFilename;
    }

    private static boolean isQueuedResponse(String message) {
        int end = message.length();
        while (end > 0 && message.charAt(end - 1) == '.') {
            end--;
        }
        return message.substring(0, end).equalsIgnoreCase("Queued");
    }

    private static void completeDownloadEnqueue(CompletableFuture<Boolean> enqueued, TransferState state) {
        if (state.equals(TransferState.QUEUED.or(TransferState.REMOTELY))) {
            enqueued.complete(true);
        } else if (state.contains(TransferState.COMPLETED)) {
            enqueued.complete(false);
        }
    }

    private static CompletableFuture<CompletableFuture<Transfer>> nestedDownloadWhenEnqueued(
            CompletableFuture<Boolean> enqueued, CompletableFuture<Transfer> download) {
        return enqueued.thenCompose(success -> {
            if (success) {
                return CompletableFuture.completedFuture(download);
            }
            return download.thenApply(ignored -> download);
        });
    }

    private CompletableFuture<Transfer> downloadToStreamAsync(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            int token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        TransferOptions operationOptions = transferOptions == null ? new TransferOptions() : transferOptions;
        TransferInternal download = new TransferInternal(
                TransferDirection.DOWNLOAD, requestedUsername, remoteFilename, token, operationOptions);
        download.setStartOffset(startOffset);
        download.setSize(size);
        String uniqueKey = downloadUniqueKey(requestedUsername, remoteFilename);

        if (uniqueKeys.putIfAbsent(uniqueKey, true) != null) {
            return CompletableFuture.failedFuture(new DuplicateTransferException(
                    "Duplicate download of " + remoteFilename + " from " + requestedUsername + " aborted"));
        }
        if (downloads.putIfAbsent(token, download) != null) {
            uniqueKeys.remove(uniqueKey);
            return CompletableFuture.failedFuture(new DuplicateTransferException(
                    "Duplicate download of " + remoteFilename + " from " + requestedUsername + " aborted"));
        }

        DownloadOperation operation =
                new DownloadOperation(download, outputStreamFactory, operationOptions, cancellationSignal, uniqueKey);
        return NetworkExecutor.supplyAsync(operation::execute);
    }

    private final class DownloadOperation {
        private final TransferInternal download;
        private final DownloadStreamFactory outputStreamFactory;
        private final TransferOptions transferOptions;
        private final CancellationSignal cancellationSignal;
        private final String uniqueKey;
        private final AtomicBoolean globalPermit = new AtomicBoolean();
        private final CompletableFuture<Void> disconnected = new CompletableFuture<>();
        private final WaitKey transferStartRequestedWaitKey;
        private TransferState lastState = TransferState.NONE;
        private InetSocketAddress endpoint;
        private Connection connection;
        private OutputStream outputStream;
        private PositionTrackingOutputStream trackingStream;
        private ConnectionEventListener<ConnectionDataEvent> dataReadListener;
        private ConnectionEventListener<ConnectionDisconnectedEvent> disconnectedListener;

        private DownloadOperation(
                TransferInternal download,
                DownloadStreamFactory outputStreamFactory,
                TransferOptions transferOptions,
                CancellationSignal cancellationSignal,
                String uniqueKey) {
            this.download = download;
            this.outputStreamFactory = outputStreamFactory;
            this.transferOptions = transferOptions;
            this.cancellationSignal = cancellationSignal;
            this.uniqueKey = uniqueKey;
            transferStartRequestedWaitKey =
                    new WaitKey(MessageCode.Peer.TRANSFER_REQUEST, download.getUsername(), download.getFilename());
        }

        private Transfer execute() {
            try {
                updateState(TransferState.QUEUED.or(TransferState.LOCALLY));
                await(acquirePermit(globalDownloadSemaphore, cancellationSignal));
                globalPermit.set(true);
                diagnostic.debug("Global download semaphore for file "
                        + filenameOnly(download.getFilename()) + " to "
                        + download.getUsername() + " acquired");

                endpoint = await(getUserEndpointOperation(download.getUsername(), cancellationSignal));
                MessageConnection peerConnection = await(peerConnectionManager.getOrAddMessageConnectionAsync(
                        download.getUsername(), endpoint, cancellationSignal));
                diagnostic.debug("Fetched peer connection for download of "
                        + filenameOnly(download.getFilename()) + " from "
                        + download.getUsername() + " (id: " + peerConnection.getId()
                        + ", state: " + peerConnection.getState() + ")");

                CompletableFuture<TransferResponse> transferRequestAcknowledged = waiter.waitAsync(
                        new WaitKey(MessageCode.Peer.TRANSFER_RESPONSE, download.getUsername(), download.getToken()),
                        TransferResponse.class,
                        options.getPeerConnectionOptions().getInactivityTimeout(),
                        cancellationSignal);
                CompletableFuture<TransferRequest> transferStartRequested = waiter.waitIndefinitelyAsync(
                        transferStartRequestedWaitKey, TransferRequest.class, cancellationSignal);

                await(invokeMessageWrite(
                        peerConnection,
                        new TransferRequest(TransferDirection.DOWNLOAD, download.getToken(), download.getFilename()),
                        cancellationSignal));
                diagnostic.debug("Wrote transfer request for download of "
                        + filenameOnly(download.getFilename()) + " from "
                        + download.getUsername() + " (id: " + peerConnection.getId()
                        + ", state: " + peerConnection.getState() + ")");
                updateState(TransferState.REQUESTED);

                TransferResponse acknowledgement = await(transferRequestAcknowledged);
                diagnostic.debug("Received transfer request ACK for download of "
                        + filenameOnly(download.getFilename()) + " from "
                        + download.getUsername() + ": allowed: " + acknowledgement.isAllowed()
                        + ", message: " + acknowledgement.getMessage()
                        + " (token: " + download.getToken() + ")");
                if (acknowledgement.isAllowed()) {
                    peerConnection = beginImmediateDownload(acknowledgement, peerConnection);
                } else if (!isQueuedResponse(acknowledgement.getMessage())) {
                    throw new TransferRejectedException("Transfer rejected: " + acknowledgement.getMessage());
                } else {
                    peerConnection = beginQueuedDownload(transferStartRequested, peerConnection);
                }

                bindConnectionEvents();
                outputStream =
                        Objects.requireNonNull(await(outputStreamFactory.openAsync()), "outputStreamFactory result");
                positionOutputStream();
                trackingStream = new PositionTrackingOutputStream(
                        outputStream,
                        determineOutputPosition(
                                outputStream,
                                transferOptions.isSeekOutputStreamAutomatically() ? download.getStartOffset() : 0));
                readTransfer();

                updateProgress(currentOutputPosition());
                updateState(TransferState.COMPLETED.or(TransferState.SUCCEEDED));
                diagnostic.info("Download of " + filenameOnly(download.getFilename())
                        + " from " + download.getUsername() + " complete ("
                        + currentOutputPosition() + " of " + download.getSize() + " bytes).");
                connection.disconnect("Transfer complete");
                return download.toTransfer();
            } catch (Throwable failure) {
                Throwable cause = unwrap(failure);
                handleFailure(cause);
                throw new CompletionException(mapDownloadFailure(cause));
            } finally {
                cleanup();
            }
        }

        private MessageConnection beginImmediateDownload(
                TransferResponse acknowledgement, MessageConnection peerConnection) {
            validateRemoteSize(acknowledgement.getFileSize());
            updateState(TransferState.QUEUED.or(TransferState.REMOTELY));
            if (download.getSize() == null) {
                download.setSize(acknowledgement.getFileSize());
            }
            updateState(TransferState.INITIALIZING);
            connection = await(peerConnectionManager.getTransferConnectionAsync(
                    download.getUsername(), endpoint, acknowledgement.getToken(), cancellationSignal));
            diagnostic.debug("Fetched transfer connection for download of "
                    + filenameOnly(download.getFilename()) + " from "
                    + download.getUsername() + " (id: " + connection.getId()
                    + ", state: " + connection.getState() + ")");
            download.setConnection(connection);
            return peerConnection;
        }

        private MessageConnection beginQueuedDownload(
                CompletableFuture<TransferRequest> transferStartRequested, MessageConnection peerConnection) {
            updateState(TransferState.QUEUED.or(TransferState.REMOTELY));
            TransferRequest request = await(transferStartRequested);
            validateRemoteSize(request.getFileSize());
            if (download.getSize() == null) {
                download.setSize(request.getFileSize());
            }
            download.setRemoteToken(request.getToken());
            updateState(TransferState.INITIALIZING);

            MessageConnection refreshed = await(peerConnectionManager.getOrAddMessageConnectionAsync(
                    download.getUsername(), endpoint, cancellationSignal));
            diagnostic.debug("Fetched peer connection for download of "
                    + filenameOnly(download.getFilename()) + " from "
                    + download.getUsername() + " (id: " + refreshed.getId()
                    + ", state: " + refreshed.getState() + ")");
            CompletableFuture<Connection> connectionTask = peerConnectionManager.awaitTransferConnectionAsync(
                    download.getUsername(), download.getFilename(), download.getRemoteToken(), cancellationSignal);
            await(invokeMessageWrite(
                    refreshed,
                    new TransferResponse(
                            download.getRemoteToken(), download.getSize() == null ? 0 : download.getSize()),
                    cancellationSignal));
            try {
                connection = await(connectionTask);
                diagnostic.debug("Fetched transfer connection for download of "
                        + filenameOnly(download.getFilename()) + " from "
                        + download.getUsername() + " (id: " + connection.getId()
                        + ", state: " + connection.getState() + ")");
            } catch (Throwable failure) {
                Throwable cause = unwrap(failure);
                if (!(cause instanceof ConnectionException)) {
                    throw failure;
                }
                // The remote client never initiated the transfer connection, so initiate one from
                // this end. The remote client in this scenario is most likely Nicotine+.
                diagnostic.warning("Attempting to initiate a second-chance transfer connection to "
                        + download.getUsername() + " for download of " + download.getFilename());
                connection = await(peerConnectionManager.getTransferConnectionAsync(
                        download.getUsername(), endpoint, download.getRemoteToken(), cancellationSignal));
                diagnostic.warning("Successfully established a second-chance transfer connection to "
                        + download.getUsername() + " for download of " + download.getFilename());
            }
            download.setConnection(connection);
            return refreshed;
        }

        private void validateRemoteSize(long remoteSize) {
            if (download.getSize() != null && download.getSize() != remoteSize) {
                throw new TransferSizeMismatchException(
                        "Transfer aborted: the remote size of "
                                + remoteSize
                                + " does not match expected size "
                                + download.getSize(),
                        download.getSize(),
                        remoteSize);
            }
        }

        private void bindConnectionEvents() {
            dataReadListener =
                    (sender, eventData) -> updateProgress(download.getStartOffset() + eventData.getCurrentLength());
            disconnectedListener = (sender, eventData) -> {
                Throwable failure = eventData.getException();
                if (failure instanceof CancellationException || failure instanceof TimeoutException) {
                    disconnected.completeExceptionally(failure);
                } else {
                    disconnected.completeExceptionally(
                            new ConnectionException("Transfer failed: " + eventData.getMessage(), failure));
                }
            };
            connection.addDataReadListener(dataReadListener);
            connection.addDisconnectedListener(disconnectedListener);
        }

        private void positionOutputStream() {
            if (download.getStartOffset() <= 0 || !transferOptions.isSeekOutputStreamAutomatically()) {
                return;
            }
            diagnostic.debug("Seeking output stream for download of "
                    + filenameOnly(download.getFilename()) + " from "
                    + download.getUsername() + " to starting offset of "
                    + download.getStartOffset() + " bytes");
            try {
                seekOutputStream(outputStream, download.getStartOffset());
            } catch (IOException failure) {
                throw new TransferStreamException(
                        "Requested non-zero start offset but output " + "stream does not support seeking", failure);
            }
        }

        private void readTransfer() {
            try (CancellationController linkedController = new CancellationController();
                    CancellationSubscription registration = cancellationSignal.register(linkedController::cancel)) {
                CancellationSignal linkedToken = linkedController.getSignal();
                diagnostic.debug("Seeking download of " + filenameOnly(download.getFilename())
                        + " from " + download.getUsername() + " to starting offset of "
                        + download.getStartOffset() + " bytes");
                byte[] offset = ByteBuffer.allocate(8)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putLong(download.getStartOffset())
                        .array();
                await(connection.writeAsync(offset, linkedToken));
                updateState(TransferState.IN_PROGRESS);
                updateProgress(download.getStartOffset());

                CompletableFuture<Void> read = connection.readAsync(
                        download.getSize() - download.getStartOffset(),
                        trackingStream,
                        (requestedBytes, governorToken) -> transferOptions
                                .getGovernor()
                                .grantAsync(download.toTransfer(), requestedBytes, governorToken)
                                .thenCompose(granted ->
                                        downloadTokenBucket.getAsync(Math.min(requestedBytes, granted), governorToken)),
                        (attemptedBytes, grantedBytes, transferredBytes) -> {
                            if (transferOptions.getReporter() != null) {
                                transferOptions
                                        .getReporter()
                                        .report(download.toTransfer(), attemptedBytes, grantedBytes, transferredBytes);
                            }
                            downloadTokenBucket.returnTokens(grantedBytes - transferredBytes);
                        },
                        linkedToken);

                CompletableFuture<Integer> readRace = read.handle((ignored, failure) -> 0);
                CompletableFuture<Integer> disconnectRace = disconnected.handle((ignored, failure) -> 1);
                CompletableFuture<Integer> remoteRace =
                        download.getRemoteTaskCompletionSource().handle((ignored, failure) -> 2);
                int winner = await(CompletableFuture.anyOf(readRace, disconnectRace, remoteRace)
                        .thenApply(value -> (Integer) value));
                linkedController.cancel();
                if (winner == 2) {
                    await(download.getRemoteTaskCompletionSource());
                } else if (winner == 1) {
                    await(disconnected);
                }
                await(read);
            }
        }

        private void handleFailure(Throwable failure) {
            if (failure instanceof TransferRejectedException) {
                download.setException(failure);
                updateState(TransferState.COMPLETED.or(TransferState.REJECTED));
                return;
            }
            if (failure instanceof TransferSizeMismatchException) {
                download.setException(failure);
                updateState(TransferState.COMPLETED.or(TransferState.ABORTED));
                return;
            }
            if (failure instanceof CancellationException) {
                disconnectTransfer("Transfer cancelled", failure);
                download.setException(failure);
                updateProgress(currentOutputPosition());
                updateState(TransferState.COMPLETED.or(TransferState.CANCELLED));
                return;
            }
            if (failure instanceof TimeoutException) {
                disconnectTransfer("Transfer timed out", failure);
                download.setException(failure);
                updateProgress(currentOutputPosition());
                updateState(TransferState.COMPLETED.or(TransferState.TIMED_OUT));
                return;
            }
            disconnectTransfer("Transfer error", failure);
            download.setException(failure);
            updateProgress(currentOutputPosition());
            updateState(TransferState.COMPLETED.or(TransferState.ERRORED));
        }

        private Throwable mapDownloadFailure(Throwable failure) {
            if (failure instanceof TransferRejectedException
                    || failure instanceof TransferSizeMismatchException
                    || failure instanceof CancellationException
                    || failure instanceof TimeoutException
                    || failure instanceof UserOfflineException) {
                return failure;
            }
            return new SoulseekClientException(
                    "Failed to download file "
                            + download.getFilename()
                            + " from user " + download.getUsername()
                            + ": " + failureMessage(failure),
                    failure);
        }

        private void disconnectTransfer(String message, Throwable failure) {
            if (connection != null) {
                connection.disconnect(
                        message, failure instanceof Exception exception ? exception : new RuntimeException(failure));
            }
        }

        private void cleanup() {
            try {
                try {
                    waiter.cancel(transferStartRequestedWaitKey);
                } catch (Throwable failure) {
                    diagnostic.warning(
                            "Failed to cancel wait for key "
                                    + transferStartRequestedWaitKey
                                    + ": " + failureMessage(failure),
                            failure);
                }
                try {
                    unbindConnectionEvents();
                } catch (Throwable failure) {
                    diagnostic.warning(
                            "Failed to remove transfer connection "
                                    + "listeners for file "
                                    + download.getFilename() + " from user "
                                    + download.getUsername() + ": "
                                    + failureMessage(failure),
                            failure);
                }
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Throwable failure) {
                        diagnostic.warning(
                                "Failed to dispose transfer connection "
                                        + "for file "
                                        + download.getFilename()
                                        + " from user "
                                        + download.getUsername() + ": "
                                        + failureMessage(failure),
                                failure);
                    }
                }
                determineFinalOutputPosition();
                if (transferOptions.isDisposeOutputStreamOnCompletion() && outputStream != null) {
                    try {
                        try {
                            outputStream.flush();
                        } finally {
                            outputStream.close();
                        }
                    } catch (Throwable failure) {
                        diagnostic.warning(
                                "Failed to finalize output stream for "
                                        + "file "
                                        + filenameOnly(download.getFilename())
                                        + " from "
                                        + download.getUsername() + ": "
                                        + failureMessage(failure),
                                failure);
                    }
                }
            } finally {
                if (globalPermit.compareAndSet(true, false)) {
                    try {
                        globalDownloadSemaphore.release();
                    } catch (Throwable failure) {
                        diagnostic.warning(
                                "Failed to release global download "
                                        + "semaphore for file "
                                        + filenameOnly(download.getFilename())
                                        + " from "
                                        + download.getUsername() + ": "
                                        + failureMessage(failure),
                                failure);
                    }
                }
                downloads.remove(download.getToken(), download);
                uniqueKeys.remove(uniqueKey);
            }
        }

        private void unbindConnectionEvents() {
            if (connection == null) {
                return;
            }
            if (dataReadListener != null) {
                connection.removeDataReadListener(dataReadListener);
            }
            if (disconnectedListener != null) {
                connection.removeDisconnectedListener(disconnectedListener);
            }
        }

        private void updateState(TransferState state) {
            download.setState(state);
            Transfer transfer = download.toTransfer();
            TransferStateChangedEvent eventData = new TransferStateChangedEvent(lastState, transfer);
            TransferState previous = lastState;
            lastState = state;
            if (transferOptions.getStateChanged() != null) {
                transferOptions.getStateChanged().onStateChanged(new TransferStateChange(previous, transfer));
            }
            raise(Event.TRANSFER_STATE_CHANGED, eventData);
        }

        private void updateProgress(long bytesDownloaded) {
            long previous = download.getBytesTransferred();
            download.updateProgress(bytesDownloaded);
            Transfer transfer = download.toTransfer();
            if (transferOptions.getProgressUpdated() != null) {
                transferOptions.getProgressUpdated().onProgressUpdated(new TransferProgressUpdate(previous, transfer));
            }
            raise(Event.TRANSFER_PROGRESS_UPDATED, new TransferProgressUpdatedEvent(previous, transfer));
        }

        private long currentOutputPosition() {
            if (trackingStream != null) {
                return trackingStream.getPosition();
            }
            if (outputStream != null) {
                try {
                    return determineOutputPosition(outputStream, 0);
                } catch (Throwable ignored) {
                    return 0;
                }
            }
            return 0;
        }

        private long determineFinalOutputPosition() {
            if (outputStream == null) {
                return 0;
            }
            try {
                return determineOutputPosition(outputStream, trackingStream == null ? 0 : trackingStream.getPosition());
            } catch (Throwable failure) {
                diagnostic.warning(
                        "Failed to determine final position of output "
                                + "stream for file "
                                + filenameOnly(download.getFilename())
                                + " from " + download.getUsername() + ": "
                                + failureMessage(failure),
                        failure);
                return 0;
            }
        }
    }

    private void validateUploadUniqueness(String requestedUsername, String remoteFilename, int token) {
        if (uploads.containsKey(token) || downloads.containsKey(token)) {
            throw new DuplicateTokenException("The specified or generated token " + token + " is already in progress");
        }
        boolean duplicate = uploads.values().stream()
                .anyMatch(upload -> Objects.equals(upload.getUsername(), requestedUsername)
                        && Objects.equals(upload.getFilename(), remoteFilename));
        if (duplicate || uniqueKeys.containsKey(uploadUniqueKey(requestedUsername, remoteFilename))) {
            throw new DuplicateTransferException("An active or queued upload of "
                    + remoteFilename + " to " + requestedUsername
                    + " is already in progress");
        }
    }

    private CompletableFuture<Transfer> uploadFromStreamAsync(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            int token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        TransferOptions operationOptions = transferOptions == null ? new TransferOptions() : transferOptions;
        TransferInternal upload = new TransferInternal(
                TransferDirection.UPLOAD, requestedUsername, remoteFilename, token, operationOptions);
        upload.setSize(size);
        String uniqueKey = uploadUniqueKey(requestedUsername, remoteFilename);

        if (uniqueKeys.putIfAbsent(uniqueKey, true) != null) {
            return CompletableFuture.failedFuture(new DuplicateTransferException(
                    "Duplicate upload of " + remoteFilename + " to " + requestedUsername + " aborted"));
        }
        if (uploads.putIfAbsent(token, upload) != null) {
            uniqueKeys.remove(uniqueKey);
            return CompletableFuture.failedFuture(new DuplicateTransferException(
                    "Duplicate upload of " + remoteFilename + " to " + requestedUsername + " aborted"));
        }

        UploadOperation operation =
                new UploadOperation(upload, inputStreamFactory, operationOptions, cancellationSignal, uniqueKey);
        return NetworkExecutor.supplyAsync(operation::execute);
    }

    private static String uploadUniqueKey(String requestedUsername, String remoteFilename) {
        return "Upload:" + requestedUsername + ":" + remoteFilename;
    }

    private final class UploadOperation {
        private final TransferInternal upload;
        private final UploadStreamFactory inputStreamFactory;
        private final TransferOptions transferOptions;
        private final CancellationSignal cancellationSignal;
        private final String uniqueKey;
        private final AtomicBoolean perUserPermit = new AtomicBoolean();
        private final AtomicBoolean slot = new AtomicBoolean();
        private final AtomicBoolean globalPermit = new AtomicBoolean();
        private final CompletableFuture<Void> disconnected = new CompletableFuture<>();
        private TransferState lastState = TransferState.NONE;
        private Semaphore perUserSemaphore;
        private InetSocketAddress endpoint;
        private Connection connection;
        private InputStream inputStream;
        private PositionTrackingInputStream trackingStream;
        private ConnectionEventListener<ConnectionDataEvent> dataWrittenListener;
        private ConnectionEventListener<ConnectionDisconnectedEvent> disconnectedListener;

        private UploadOperation(
                TransferInternal upload,
                UploadStreamFactory inputStreamFactory,
                TransferOptions transferOptions,
                CancellationSignal cancellationSignal,
                String uniqueKey) {
            this.upload = upload;
            this.inputStreamFactory = inputStreamFactory;
            this.transferOptions = transferOptions;
            this.cancellationSignal = cancellationSignal;
            this.uniqueKey = uniqueKey;
        }

        private Transfer execute() {
            try {
                await(acquirePermit(uploadSemaphoreSyncRoot, cancellationSignal));
                CompletableFuture<Void> perUserWait;
                try {
                    perUserSemaphore = uploadSemaphores.computeIfAbsent(
                            upload.getUsername(),
                            ignored -> new Semaphore(options.getMaximumConcurrentUploadsPerUser()));
                    perUserWait = acquirePermit(perUserSemaphore, cancellationSignal);
                } finally {
                    uploadSemaphoreSyncRoot.release();
                }

                updateState(TransferState.QUEUED.or(TransferState.LOCALLY));

                await(perUserWait);
                perUserPermit.set(true);
                diagnostic.debug("Upload semaphore for file "
                        + filenameOnly(upload.getFilename()) + " to "
                        + upload.getUsername() + " acquired");

                try {
                    await(transferOptions.getSlotAwaiter().awaitSlotAsync(upload.toTransfer(), cancellationSignal));
                    slot.set(true);
                    diagnostic.debug("Upload slot for file "
                            + filenameOnly(upload.getFilename()) + " to "
                            + upload.getUsername() + " acquired");
                } catch (Throwable failure) {
                    Throwable cause = unwrap(failure);
                    if (cause instanceof CancellationException) {
                        throw cause;
                    }
                    throw new TransferException(
                            "Failed to acquire an upload slot for file "
                                    + filenameOnly(upload.getFilename())
                                    + " to " + upload.getUsername() + ": "
                                    + failureMessage(cause),
                            cause);
                }

                await(acquirePermit(globalUploadSemaphore, cancellationSignal));
                globalPermit.set(true);
                diagnostic.debug("Global upload semaphore for file "
                        + filenameOnly(upload.getFilename()) + " to "
                        + upload.getUsername() + " acquired");

                endpoint = await(getUserEndpointOperation(upload.getUsername(), cancellationSignal));
                MessageConnection messageConnection = await(peerConnectionManager.getOrAddMessageConnectionAsync(
                        upload.getUsername(), endpoint, cancellationSignal));
                diagnostic.debug("Fetched peer connection for upload of "
                        + filenameOnly(upload.getFilename()) + " to "
                        + upload.getUsername() + " (id: " + messageConnection.getId()
                        + ", state: " + messageConnection.getState() + ")");

                CompletableFuture<TransferResponse> transferRequestAcknowledged = waiter.waitAsync(
                        new WaitKey(MessageCode.Peer.TRANSFER_RESPONSE, upload.getUsername(), upload.getToken()),
                        TransferResponse.class,
                        options.getPeerConnectionOptions().getInactivityTimeout(),
                        cancellationSignal);
                await(invokeMessageWrite(
                        messageConnection,
                        new TransferRequest(
                                TransferDirection.UPLOAD, upload.getToken(), upload.getFilename(), upload.getSize()),
                        cancellationSignal));
                diagnostic.debug("Wrote transfer request for upload of "
                        + filenameOnly(upload.getFilename()) + " to "
                        + upload.getUsername() + " (id: " + messageConnection.getId()
                        + ", state: " + messageConnection.getState() + ")");
                updateState(TransferState.REQUESTED);

                TransferResponse acknowledgement = await(transferRequestAcknowledged);
                diagnostic.debug("Received transfer request ACK for upload of "
                        + filenameOnly(upload.getFilename()) + " to "
                        + upload.getUsername() + ": allowed: " + acknowledgement.isAllowed()
                        + ", message: " + acknowledgement.getMessage()
                        + " (token: " + upload.getToken() + ")");
                if (!acknowledgement.isAllowed()) {
                    throw new TransferRejectedException("Transfer rejected: " + acknowledgement.getMessage());
                }

                updateState(TransferState.INITIALIZING);
                connection = await(peerConnectionManager.getTransferConnectionAsync(
                        upload.getUsername(), endpoint, upload.getToken(), cancellationSignal));
                diagnostic.debug("Fetched transfer connection for upload of "
                        + filenameOnly(upload.getFilename()) + " to "
                        + upload.getUsername() + " (id: " + connection.getId()
                        + ", state: " + connection.getState() + ")");
                upload.setConnection(connection);
                bindConnectionEvents();

                readStartOffset();
                if (upload.getStartOffset() > upload.getSize()) {
                    throw new TransferException("Requested start offset of "
                            + upload.getStartOffset()
                            + " bytes exceeds file length of "
                            + upload.getSize() + " bytes");
                }

                diagnostic.debug("Resolving input stream for upload of " + filenameOnly(upload.getFilename()) + " to "
                        + upload.getUsername());
                inputStream = Objects.requireNonNull(
                        await(inputStreamFactory.openAsync(upload.getStartOffset())), "inputStreamFactory result");
                positionInputStream();
                trackingStream = new PositionTrackingInputStream(
                        inputStream, determinePosition(inputStream, upload.getStartOffset()));

                updateState(TransferState.IN_PROGRESS);
                updateProgress(upload.getStartOffset());
                writeAndAwaitDisconnectRace();
                linger();

                updateProgress(currentStreamPosition());
                updateState(TransferState.COMPLETED.or(TransferState.SUCCEEDED));
                return upload.toTransfer();
            } catch (Throwable failure) {
                Throwable cause = unwrap(failure);
                handleFailure(cause);
                throw new CompletionException(mapUploadFailure(cause));
            } finally {
                cleanup();
            }
        }

        private void bindConnectionEvents() {
            dataWrittenListener =
                    (sender, eventData) -> updateProgress(upload.getStartOffset() + eventData.getCurrentLength());
            disconnectedListener = (sender, eventData) -> {
                Throwable failure = eventData.getException();
                if (failure instanceof CancellationException || failure instanceof TimeoutException) {
                    disconnected.completeExceptionally(failure);
                } else {
                    disconnected.completeExceptionally(
                            new ConnectionException("Transfer failed: " + eventData.getMessage(), failure));
                }
            };
            connection.addDataWrittenListener(dataWrittenListener);
            connection.addDisconnectedListener(disconnectedListener);
        }

        private void readStartOffset() {
            try {
                byte[] bytes = await(connection.readAsync(8, cancellationSignal));
                if (bytes.length != 8) {
                    throw new IOException("Expected 8 bytes but received " + bytes.length);
                }
                upload.setStartOffset(
                        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong());
            } catch (Throwable failure) {
                Throwable cause = unwrap(failure);
                diagnostic.debug("Failed to read start offset for upload of "
                        + filenameOnly(upload.getFilename()) + " to "
                        + upload.getUsername() + ": " + failureMessage(cause));
                if (cause instanceof CancellationException || cause instanceof TimeoutException) {
                    throw new CompletionException(cause);
                }
                throw new MessageReadException("Failed to read transfer start offset: " + failureMessage(cause), cause);
            }
        }

        private void positionInputStream() {
            if (upload.getStartOffset() <= 0 || !transferOptions.isSeekInputStreamAutomatically()) {
                return;
            }
            diagnostic.debug("Seeking input stream for upload of "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + " to starting offset of "
                    + upload.getStartOffset() + " bytes");
            try {
                seekInputStream(inputStream, upload.getStartOffset());
            } catch (IOException failure) {
                throw new TransferStreamException(
                        "Requested non-zero start offset but input " + "stream does not support seeking", failure);
            }
        }

        private void writeAndAwaitDisconnectRace() {
            long remaining = upload.getSize() - upload.getStartOffset();
            CompletableFuture<Void> write = remaining == 0
                    ? CompletableFuture.completedFuture(null)
                    : connection.writeAsync(
                            remaining,
                            trackingStream,
                            (requestedBytes, governorToken) -> transferOptions
                                    .getGovernor()
                                    .grantAsync(upload.toTransfer(), requestedBytes, governorToken)
                                    .thenCompose(granted -> uploadTokenBucket.getAsync(
                                            Math.min(requestedBytes, granted), cancellationSignal)),
                            (attemptedBytes, grantedBytes, transferredBytes) -> {
                                if (transferOptions.getReporter() != null) {
                                    transferOptions
                                            .getReporter()
                                            .report(
                                                    upload.toTransfer(),
                                                    attemptedBytes,
                                                    grantedBytes,
                                                    transferredBytes);
                                }
                                uploadTokenBucket.returnTokens(grantedBytes - transferredBytes);
                            },
                            cancellationSignal);
            CompletableFuture<Object> first = CompletableFuture.anyOf(write, disconnected);
            await(first);
            if (disconnected.isCompletedExceptionally() && !write.isDone()) {
                await(disconnected);
            }
            await(write);
        }

        private void linger() {
            long deadline = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(Math.max(0, transferOptions.getMaximumLingerTime()));
            try {
                while (!cancellationSignal.isCancellationRequested()) {
                    long remainingNanos = deadline - System.nanoTime();
                    if (remainingNanos <= 0) {
                        connection.disconnect("Transfer complete, maximum linger " + "time exceeded");
                        return;
                    }
                    long remainingMillis = Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
                    try {
                        await(connection
                                .readAsync(1, cancellationSignal)
                                .orTimeout(remainingMillis, TimeUnit.MILLISECONDS));
                    } catch (Throwable failure) {
                        Throwable cause = unwrap(failure);
                        if (cause instanceof TimeoutException) {
                            connection.disconnect("Transfer complete, maximum " + "linger time exceeded");
                            return;
                        }
                        throw failure;
                    }
                    await(CompletableFuture.runAsync(
                            () -> {}, CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS)));
                }
                cancellationSignal.throwIfCancellationRequested();
            } catch (Throwable failure) {
                if (!(unwrap(failure) instanceof ConnectionReadException)) {
                    throw failure;
                }
            }
        }

        private void handleFailure(Throwable failure) {
            if (failure instanceof TransferRejectedException) {
                upload.setException(failure);
                updateState(TransferState.COMPLETED.or(TransferState.REJECTED));
                return;
            }
            if (failure instanceof CancellationException) {
                disconnectTransfer("Transfer cancelled", failure);
                upload.setException(failure);
                updateProgress(currentStreamPosition());
                updateState(TransferState.COMPLETED.or(TransferState.CANCELLED));
                return;
            }
            if (failure instanceof TimeoutException) {
                disconnectTransfer("Transfer timed out", failure);
                upload.setException(failure);
                updateProgress(currentStreamPosition());
                updateState(TransferState.COMPLETED.or(TransferState.TIMED_OUT));
                return;
            }
            disconnectTransfer("Transfer error", failure);
            upload.setException(failure);
            updateProgress(currentStreamPosition());
            updateState(TransferState.COMPLETED.or(TransferState.ERRORED));
        }

        private Throwable mapUploadFailure(Throwable failure) {
            if (failure instanceof TransferRejectedException
                    || failure instanceof CancellationException
                    || failure instanceof TimeoutException
                    || failure instanceof UserOfflineException) {
                return failure;
            }
            return new SoulseekClientException(
                    "Failed to upload file " + upload.getFilename()
                            + " to user " + upload.getUsername() + ": "
                            + failureMessage(failure),
                    failure);
        }

        private void disconnectTransfer(String message, Throwable failure) {
            if (connection != null) {
                connection.disconnect(
                        message, failure instanceof Exception exception ? exception : new RuntimeException(failure));
            }
        }

        private void cleanup() {
            try {
                unbindConnectionEvents();
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Throwable ignored) {
                        // Best-effort connection cleanup.
                    }
                }
                currentStreamPosition();
                if (transferOptions.isDisposeInputStreamOnCompletion() && inputStream != null) {
                    try {
                        inputStream.close();
                    } catch (Throwable ignored) {
                        // Best-effort stream cleanup.
                    }
                }
                if (!upload.getState().contains(TransferState.SUCCEEDED)) {
                    notifyUploadFailure();
                }
            } finally {
                releasePermits();
                uploads.remove(upload.getToken(), upload);
                uniqueKeys.remove(uniqueKey);
            }
        }

        private void unbindConnectionEvents() {
            if (connection == null) {
                return;
            }
            if (dataWrittenListener != null) {
                connection.removeDataWrittenListener(dataWrittenListener);
            }
            if (disconnectedListener != null) {
                connection.removeDisconnectedListener(disconnectedListener);
            }
        }

        private void notifyUploadFailure() {
            try {
                InetSocketAddress currentEndpoint =
                        await(getUserEndpointOperation(upload.getUsername(), CancellationSignal.none()));
                MessageConnection messageConnection = await(peerConnectionManager.getOrAddMessageConnectionAsync(
                        upload.getUsername(), currentEndpoint, CancellationSignal.none()));
                OutgoingMessage message = upload.getState().contains(TransferState.CANCELLED)
                        ? new UploadDenied(upload.getFilename(), "Cancelled")
                        : new UploadFailed(upload.getFilename());
                await(invokeMessageWrite(messageConnection, message, CancellationSignal.none()));
            } catch (Throwable ignored) {
                // Failure notification is intentionally best effort.
            }
        }

        private void releasePermits() {
            if (perUserPermit.compareAndSet(true, false)) {
                perUserSemaphore.release();
                diagnostic.debug("Upload semaphore for file "
                        + filenameOnly(upload.getFilename()) + " to "
                        + upload.getUsername() + " released");
            }
            if (slot.compareAndSet(true, false)) {
                diagnostic.debug("Upload slot for file "
                        + filenameOnly(upload.getFilename()) + " to "
                        + upload.getUsername() + " released");
                if (transferOptions.getSlotReleased() != null) {
                    try {
                        Thread.sleep(10);
                        transferOptions.getSlotReleased().onSlotReleased(upload.toTransfer());
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                    } catch (Throwable ignored) {
                        // Slot-release callbacks cannot block cleanup.
                    }
                }
            }
            if (globalPermit.compareAndSet(true, false)) {
                globalUploadSemaphore.release();
                diagnostic.debug("Global upload semaphore for file "
                        + filenameOnly(upload.getFilename()) + " to "
                        + upload.getUsername() + " released");
            }
        }

        private void updateState(TransferState state) {
            upload.setState(state);
            Transfer transfer = upload.toTransfer();
            TransferStateChangedEvent eventData = new TransferStateChangedEvent(lastState, transfer);
            TransferState previous = lastState;
            lastState = state;
            if (transferOptions.getStateChanged() != null) {
                transferOptions.getStateChanged().onStateChanged(new TransferStateChange(previous, transfer));
            }
            raise(Event.TRANSFER_STATE_CHANGED, eventData);
        }

        private void updateProgress(long bytesUploaded) {
            long previous = upload.getBytesTransferred();
            upload.updateProgress(bytesUploaded);
            Transfer transfer = upload.toTransfer();
            if (transferOptions.getProgressUpdated() != null) {
                transferOptions.getProgressUpdated().onProgressUpdated(new TransferProgressUpdate(previous, transfer));
            }
            raise(Event.TRANSFER_PROGRESS_UPDATED, new TransferProgressUpdatedEvent(previous, transfer));
        }

        private long currentStreamPosition() {
            if (trackingStream != null) {
                return trackingStream.getPosition();
            }
            if (inputStream != null) {
                try {
                    return determinePosition(inputStream, 0);
                } catch (Throwable ignored) {
                    return 0;
                }
            }
            return 0;
        }
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

    private static void seekInputStream(InputStream stream, long position) throws IOException {
        if (stream instanceof PositionableInputStream positionable) {
            positionable.setPosition(position);
            return;
        }
        if (stream instanceof FileInputStream fileInputStream) {
            fileInputStream.getChannel().position(position);
            return;
        }
        if (stream instanceof ByteArrayInputStream) {
            stream.reset();
            skipFully(stream, position);
            return;
        }
        throw new IOException("Input stream is not seekable");
    }

    private static void skipFully(InputStream stream, long count) throws IOException {
        long remaining = count;
        while (remaining > 0) {
            long skipped = stream.skip(remaining);
            if (skipped <= 0) {
                if (stream.read() < 0) {
                    throw new IOException("Input stream ended before position " + count);
                }
                skipped = 1;
            }
            remaining -= skipped;
        }
    }

    private static long determinePosition(InputStream stream, long fallback) throws IOException {
        if (stream instanceof PositionableInputStream positionable) {
            return positionable.getPosition();
        }
        if (stream instanceof FileInputStream fileInputStream) {
            return fileInputStream.getChannel().position();
        }
        return fallback;
    }

    private static void seekOutputStream(OutputStream stream, long position) throws IOException {
        if (stream instanceof PositionableOutputStream positionable) {
            positionable.setPosition(position);
            return;
        }
        if (stream instanceof FileOutputStream fileOutputStream) {
            fileOutputStream.getChannel().position(position);
            return;
        }
        throw new IOException("Output stream is not seekable");
    }

    private static long determineOutputPosition(OutputStream stream, long fallback) throws IOException {
        if (stream instanceof PositionableOutputStream positionable) {
            return positionable.getPosition();
        }
        if (stream instanceof FileOutputStream fileOutputStream) {
            return fileOutputStream.getChannel().position();
        }
        return fallback;
    }

    private static String filenameOnly(String filename) {
        try {
            Path path = Path.of(filename);
            Path leaf = path.getFileName();
            return leaf == null ? filename : leaf.toString();
        } catch (Throwable ignored) {
            return filename;
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
    private static <T> T unwrapped(CompletableFuture<T> operation) {
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
                        ? downloadOperation(
                                request.getUsername(),
                                request.getRemoteFilename(),
                                request.getLocalFilename(),
                                request.getSize(),
                                request.getStartOffset(),
                                request.getToken(),
                                request.getOptions(),
                                request.getCancellationSignal())
                        : downloadOperation(
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
                        ? enqueueDownloadOperation(
                                request.getUsername(),
                                request.getRemoteFilename(),
                                request.getLocalFilename(),
                                request.getSize(),
                                request.getStartOffset(),
                                request.getToken(),
                                request.getOptions(),
                                request.getCancellationSignal())
                        : enqueueDownloadOperation(
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
                        ? uploadOperation(
                                request.getUsername(),
                                request.getRemoteFilename(),
                                request.getLocalFilename(),
                                request.getToken(),
                                request.getOptions(),
                                request.getCancellationSignal())
                        : uploadOperation(
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
                        ? enqueueUploadOperation(
                                request.getUsername(),
                                request.getRemoteFilename(),
                                request.getLocalFilename(),
                                request.getToken(),
                                request.getOptions(),
                                request.getCancellationSignal())
                        : enqueueUploadOperation(
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

    @Override
    public CompletableFuture<java.net.InetSocketAddress> resolveUserEndpoint(
            String username, CancellationSignal cancellationSignal) {
        return getUserEndpointOperation(username, cancellationSignal);
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

    // ---- Blocking public API ----------------------------------------------
    // Each of these presents one internal operation. The operations are still
    // future-shaped inside; Phase 6 inlines them as the client is decomposed.

    @Override
    public void acknowledgePrivateMessage(int privateMessageId) {
        unwrapped(server.acknowledgePrivateMessage(privateMessageId));
    }

    @Override
    public void acknowledgePrivateMessage(int privateMessageId, CancellationSignal cancellationSignal) {
        unwrapped(server.acknowledgePrivateMessage(privateMessageId, cancellationSignal));
    }

    @Override
    public void acknowledgePrivilegeNotification(int privilegeNotificationId) {
        unwrapped(server.acknowledgePrivilegeNotification(privilegeNotificationId));
    }

    @Override
    public void acknowledgePrivilegeNotification(int privilegeNotificationId, CancellationSignal cancellationSignal) {
        unwrapped(server.acknowledgePrivilegeNotification(privilegeNotificationId, cancellationSignal));
    }

    @Override
    public void addPrivateRoomMember(String roomName, String username) {
        unwrapped(rooms.addPrivateRoomMember(roomName, username));
    }

    @Override
    public void addPrivateRoomMember(String roomName, String username, CancellationSignal cancellationSignal) {
        unwrapped(rooms.addPrivateRoomMember(roomName, username, cancellationSignal));
    }

    @Override
    public void addPrivateRoomModerator(String roomName, String username) {
        unwrapped(rooms.addPrivateRoomModerator(roomName, username));
    }

    @Override
    public void addPrivateRoomModerator(String roomName, String username, CancellationSignal cancellationSignal) {
        unwrapped(rooms.addPrivateRoomModerator(roomName, username, cancellationSignal));
    }

    @Override
    public BrowseResponse browse(String username) {
        return unwrapped(users.browse(username));
    }

    @Override
    public BrowseResponse browse(String username, BrowseOptions options) {
        return unwrapped(users.browse(username, options));
    }

    @Override
    public BrowseResponse browse(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users.browse(username, cancellationSignal));
    }

    @Override
    public BrowseResponse browse(String username, BrowseOptions options, CancellationSignal cancellationSignal) {
        return unwrapped(users.browse(username, options, cancellationSignal));
    }

    @Override
    public void changePassword(String password) {
        unwrapped(server.changePassword(password));
    }

    @Override
    public void changePassword(String password, CancellationSignal cancellationSignal) {
        unwrapped(server.changePassword(password, cancellationSignal));
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
    public void connectToUser(String username) {
        unwrapped(users.connectToUser(username));
    }

    @Override
    public void connectToUser(String username, boolean invalidateCache) {
        unwrapped(users.connectToUser(username, invalidateCache));
    }

    @Override
    public void connectToUser(String username, CancellationSignal cancellationSignal) {
        unwrapped(users.connectToUser(username, cancellationSignal));
    }

    @Override
    public void connectToUser(String username, boolean invalidateCache, CancellationSignal cancellationSignal) {
        unwrapped(users.connectToUser(username, invalidateCache, cancellationSignal));
    }

    @Override
    public void dropPrivateRoomMembership(String roomName) {
        unwrapped(rooms.dropPrivateRoomMembership(roomName));
    }

    @Override
    public void dropPrivateRoomMembership(String roomName, CancellationSignal cancellationSignal) {
        unwrapped(rooms.dropPrivateRoomMembership(roomName, cancellationSignal));
    }

    @Override
    public void dropPrivateRoomOwnership(String roomName) {
        unwrapped(rooms.dropPrivateRoomOwnership(roomName));
    }

    @Override
    public void dropPrivateRoomOwnership(String roomName, CancellationSignal cancellationSignal) {
        unwrapped(rooms.dropPrivateRoomOwnership(roomName, cancellationSignal));
    }

    @Override
    public List<Directory> getDirectoryContents(String username, String directoryName) {
        return unwrapped(getDirectoryContentsOperation(username, directoryName));
    }

    @Override
    public List<Directory> getDirectoryContents(String username, String directoryName, int token) {
        return unwrapped(getDirectoryContentsOperation(username, directoryName, token));
    }

    @Override
    public List<Directory> getDirectoryContents(
            String username, String directoryName, CancellationSignal cancellationSignal) {
        return unwrapped(getDirectoryContentsOperation(username, directoryName, cancellationSignal));
    }

    @Override
    public List<Directory> getDirectoryContents(
            String username, String directoryName, Integer token, CancellationSignal cancellationSignal) {
        return unwrapped(getDirectoryContentsOperation(username, directoryName, token, cancellationSignal));
    }

    @Override
    public Integer getDownloadPlaceInQueue(String username, String filename) {
        return unwrapped(getDownloadPlaceInQueueOperation(username, filename));
    }

    @Override
    public Integer getDownloadPlaceInQueue(String username, String filename, CancellationSignal cancellationSignal) {
        return unwrapped(getDownloadPlaceInQueueOperation(username, filename, cancellationSignal));
    }

    @Override
    public Integer getPrivileges() {
        return unwrapped(server.getPrivileges());
    }

    @Override
    public Integer getPrivileges(CancellationSignal cancellationSignal) {
        return unwrapped(server.getPrivileges(cancellationSignal));
    }

    @Override
    public RoomList getRoomList() {
        return unwrapped(rooms.getRoomList());
    }

    @Override
    public RoomList getRoomList(CancellationSignal cancellationSignal) {
        return unwrapped(rooms.getRoomList(cancellationSignal));
    }

    @Override
    public InetSocketAddress getUserEndpoint(String username) {
        return unwrapped(getUserEndpointOperation(username));
    }

    @Override
    public InetSocketAddress getUserEndpoint(String username, CancellationSignal cancellationSignal) {
        return unwrapped(getUserEndpointOperation(username, cancellationSignal));
    }

    @Override
    public UserInfo getUserInfo(String username) {
        return unwrapped(users.getUserInfo(username));
    }

    @Override
    public UserInfo getUserInfo(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users.getUserInfo(username, cancellationSignal));
    }

    @Override
    public Boolean getUserPrivileged(String username) {
        return unwrapped(users.getUserPrivileged(username));
    }

    @Override
    public Boolean getUserPrivileged(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users.getUserPrivileged(username, cancellationSignal));
    }

    @Override
    public UserStatistics getUserStatistics(String username) {
        return unwrapped(users.getUserStatistics(username));
    }

    @Override
    public UserStatistics getUserStatistics(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users.getUserStatistics(username, cancellationSignal));
    }

    @Override
    public UserStatus getUserStatus(String username) {
        return unwrapped(users.getUserStatus(username));
    }

    @Override
    public UserStatus getUserStatus(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users.getUserStatus(username, cancellationSignal));
    }

    @Override
    public void grantUserPrivileges(String username, int days) {
        unwrapped(users.grantUserPrivileges(username, days));
    }

    @Override
    public void grantUserPrivileges(String username, int days, CancellationSignal cancellationSignal) {
        unwrapped(users.grantUserPrivileges(username, days, cancellationSignal));
    }

    @Override
    public RoomData joinRoom(String roomName) {
        return unwrapped(rooms.joinRoom(roomName));
    }

    @Override
    public RoomData joinRoom(String roomName, boolean isPrivate) {
        return unwrapped(rooms.joinRoom(roomName, isPrivate));
    }

    @Override
    public RoomData joinRoom(String roomName, CancellationSignal cancellationSignal) {
        return unwrapped(rooms.joinRoom(roomName, cancellationSignal));
    }

    @Override
    public RoomData joinRoom(String roomName, boolean isPrivate, CancellationSignal cancellationSignal) {
        return unwrapped(rooms.joinRoom(roomName, isPrivate, cancellationSignal));
    }

    @Override
    public void leaveRoom(String roomName) {
        unwrapped(rooms.leaveRoom(roomName));
    }

    @Override
    public void leaveRoom(String roomName, CancellationSignal cancellationSignal) {
        unwrapped(rooms.leaveRoom(roomName, cancellationSignal));
    }

    @Override
    public Long pingServer() {
        return unwrapped(server.pingServer());
    }

    @Override
    public Long pingServer(CancellationSignal cancellationSignal) {
        return unwrapped(server.pingServer(cancellationSignal));
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
    public void removePrivateRoomMember(String roomName, String username) {
        unwrapped(rooms.removePrivateRoomMember(roomName, username));
    }

    @Override
    public void removePrivateRoomMember(String roomName, String username, CancellationSignal cancellationSignal) {
        unwrapped(rooms.removePrivateRoomMember(roomName, username, cancellationSignal));
    }

    @Override
    public void removePrivateRoomModerator(String roomName, String username) {
        unwrapped(rooms.removePrivateRoomModerator(roomName, username));
    }

    @Override
    public void removePrivateRoomModerator(String roomName, String username, CancellationSignal cancellationSignal) {
        unwrapped(rooms.removePrivateRoomModerator(roomName, username, cancellationSignal));
    }

    @Override
    public void sendPrivateMessage(String username, String message) {
        unwrapped(server.sendPrivateMessage(username, message));
    }

    @Override
    public void sendPrivateMessage(String username, String message, CancellationSignal cancellationSignal) {
        unwrapped(server.sendPrivateMessage(username, message, cancellationSignal));
    }

    @Override
    public void sendRoomMessage(String roomName, String message) {
        unwrapped(rooms.sendRoomMessage(roomName, message));
    }

    @Override
    public void sendRoomMessage(String roomName, String message, CancellationSignal cancellationSignal) {
        unwrapped(rooms.sendRoomMessage(roomName, message, cancellationSignal));
    }

    @Override
    public void sendUploadSpeed(int speed) {
        unwrapped(server.sendUploadSpeed(speed));
    }

    @Override
    public void sendUploadSpeed(int speed, CancellationSignal cancellationSignal) {
        unwrapped(server.sendUploadSpeed(speed, cancellationSignal));
    }

    @Override
    public void setRoomTicker(String roomName, String message) {
        unwrapped(rooms.setRoomTicker(roomName, message));
    }

    @Override
    public void setRoomTicker(String roomName, String message, CancellationSignal cancellationSignal) {
        unwrapped(rooms.setRoomTicker(roomName, message, cancellationSignal));
    }

    @Override
    public void setSharedCounts(int directories, int files) {
        unwrapped(server.setSharedCounts(directories, files));
    }

    @Override
    public void setSharedCounts(int directories, int files, CancellationSignal cancellationSignal) {
        unwrapped(server.setSharedCounts(directories, files, cancellationSignal));
    }

    @Override
    public void setStatus(UserPresence status) {
        unwrapped(server.setStatus(status));
    }

    @Override
    public void setStatus(UserPresence status, CancellationSignal cancellationSignal) {
        unwrapped(server.setStatus(status, cancellationSignal));
    }

    @Override
    public void startPublicChat() {
        unwrapped(server.startPublicChat());
    }

    @Override
    public void startPublicChat(CancellationSignal cancellationSignal) {
        unwrapped(server.startPublicChat(cancellationSignal));
    }

    @Override
    public void stopPublicChat() {
        unwrapped(server.stopPublicChat());
    }

    @Override
    public void stopPublicChat(CancellationSignal cancellationSignal) {
        unwrapped(server.stopPublicChat(cancellationSignal));
    }

    @Override
    public void unwatchUser(String username) {
        unwrapped(users.unwatchUser(username));
    }

    @Override
    public void unwatchUser(String username, CancellationSignal cancellationSignal) {
        unwrapped(users.unwatchUser(username, cancellationSignal));
    }

    @Override
    public UserData watchUser(String username) {
        return unwrapped(users.watchUser(username));
    }

    @Override
    public UserData watchUser(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users.watchUser(username, cancellationSignal));
    }

    private static <T> T await(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (Throwable failure) {
            throw new CompletionException(unwrap(failure));
        }
    }

    private static final class PositionTrackingInputStream extends FilterInputStream {
        private long position;

        private PositionTrackingInputStream(InputStream inputStream, long initialPosition) {
            super(inputStream);
            position = initialPosition;
        }

        private long getPosition() {
            return position;
        }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value >= 0) {
                position++;
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int read = super.read(bytes, offset, length);
            if (read > 0) {
                position += read;
            }
            return read;
        }

        @Override
        public long skip(long count) throws IOException {
            long skipped = super.skip(count);
            position += skipped;
            return skipped;
        }
    }

    private static final class PositionTrackingOutputStream extends FilterOutputStream {
        private long position;

        private PositionTrackingOutputStream(OutputStream outputStream, long initialPosition) {
            super(outputStream);
            position = initialPosition;
        }

        private long getPosition() {
            return position;
        }

        @Override
        public void write(int value) throws IOException {
            out.write(value);
            position++;
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            out.write(bytes, offset, length);
            position += length;
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

    private CompletableFuture<InetSocketAddress> retrieveUserEndpoint(
            String requestedUsername, CancellationSignal cancellationSignal, UserEndpointCache cache) {
        CompletableFuture<UserAddressResponse> wait;
        try {
            wait = waiter.waitAsync(
                    new dev.slsk.common.WaitKey(MessageCode.Server.GET_PEER_ADDRESS, requestedUsername),
                    UserAddressResponse.class,
                    null,
                    cancellationSignal);
        } catch (Throwable failure) {
            return mapUserEndpointFailure(CompletableFuture.failedFuture(failure), requestedUsername);
        }
        CompletableFuture<InetSocketAddress> operation = invokeServerWrite(
                        new UserAddressRequest(requestedUsername), cancellationSignal)
                .thenCompose(ignored -> wait)
                .thenApply(response -> {
                    if (response.getIpAddress().isAnyLocalAddress()) {
                        throw new UserOfflineException("User " + requestedUsername + " appears to be offline");
                    }
                    InetSocketAddress result = response.getIpEndpoint();
                    if (cache != null) {
                        try {
                            cache.put(requestedUsername, result);
                        } catch (Throwable failure) {
                            throw new UserEndpointCacheException(
                                    "Exception retrieving or updating user "
                                            + "endpoint cache: "
                                            + failureMessage(failure),
                                    failure);
                        }
                        diagnostic.debug("Endpoint cache MISS for " + requestedUsername + ": " + result);
                    }
                    return result;
                });
        return mapUserEndpointFailure(operation, requestedUsername);
    }

    private static CompletableFuture<InetSocketAddress> mapUserEndpointFailure(
            CompletableFuture<InetSocketAddress> operation, String requestedUsername) {
        return operation.handle((result, failure) -> {
            if (failure == null) {
                return result;
            }
            Throwable cause = unwrap(failure);
            if (cause instanceof UserOfflineException
                    || cause instanceof UserEndpointCacheException
                    || cause instanceof CancellationException
                    || cause instanceof TimeoutException) {
                throw new CompletionException(cause);
            }
            throw new CompletionException(new UserEndpointException(
                    "Failed to retrieve endpoint for user " + requestedUsername + ": " + failureMessage(cause), cause));
        });
    }

    private static CacheLookupResult<InetSocketAddress> tryCacheGet(UserEndpointCache cache, String requestedUsername) {
        try {
            return cache.lookup(requestedUsername);
        } catch (Throwable failure) {
            throw new UserEndpointCacheException(
                    "Exception retrieving or updating user endpoint cache: " + failureMessage(failure), failure);
        }
    }

    @Override
    public CancellationSignal defaultToken(CancellationSignal token) {
        return token == null ? CancellationSignal.none() : token;
    }

    @FunctionalInterface
    interface ClientListenerFactory {
        Listener create(InetAddress ipAddress, int port, ConnectionOptions connectionOptions);
    }

    enum Event {
        BROWSE_PROGRESS_UPDATED,
        CONNECTED,
        DEMOTED_FROM_DISTRIBUTED_BRANCH_ROOT,
        DIAGNOSTIC_GENERATED,
        DISCONNECTED,
        DISTRIBUTED_CHILD_ADDED,
        DISTRIBUTED_CHILD_DISCONNECTED,
        DISTRIBUTED_NETWORK_RESET,
        DISTRIBUTED_NETWORK_STATE_CHANGED,
        DISTRIBUTED_PARENT_ADOPTED,
        DISTRIBUTED_PARENT_DISCONNECTED,
        DOWNLOAD_DENIED,
        DOWNLOAD_FAILED,
        EXCLUDED_SEARCH_PHRASES_RECEIVED,
        GLOBAL_MESSAGE_RECEIVED,
        KICKED_FROM_SERVER,
        LOGGED_IN,
        PRIVATE_MESSAGE_RECEIVED,
        PRIVATE_ROOM_MEMBERSHIP_ADDED,
        PRIVATE_ROOM_MEMBERSHIP_REMOVED,
        PRIVATE_ROOM_MODERATED_USER_LIST_RECEIVED,
        PRIVATE_ROOM_MODERATION_ADDED,
        PRIVATE_ROOM_MODERATION_REMOVED,
        PRIVATE_ROOM_USER_LIST_RECEIVED,
        PRIVILEGED_USER_LIST_RECEIVED,
        PRIVILEGE_NOTIFICATION_RECEIVED,
        PROMOTED_TO_DISTRIBUTED_BRANCH_ROOT,
        PUBLIC_CHAT_MESSAGE_RECEIVED,
        ROOM_JOINED,
        ROOM_LEFT,
        ROOM_LIST_RECEIVED,
        ROOM_MESSAGE_RECEIVED,
        ROOM_TICKER_ADDED,
        ROOM_TICKER_LIST_RECEIVED,
        ROOM_TICKER_REMOVED,
        SEARCH_REQUEST_RECEIVED,
        SEARCH_RESPONSE_DELIVERED,
        SEARCH_RESPONSE_DELIVERY_FAILED,
        SEARCH_RESPONSE_RECEIVED,
        SEARCH_STATE_CHANGED,
        SERVER_INFO_RECEIVED,
        STATE_CHANGED,
        TRANSFER_PROGRESS_UPDATED,
        TRANSFER_STATE_CHANGED,
        USER_CANNOT_CONNECT,
        USER_STATISTICS_CHANGED,
        USER_STATUS_CHANGED
    }
}
