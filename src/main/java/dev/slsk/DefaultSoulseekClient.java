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
import dev.slsk.exceptions.KickedFromServerException;
import dev.slsk.exceptions.ListenException;
import dev.slsk.exceptions.LoginRejectedException;
import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferReportedFailedException;
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
import dev.slsk.messaging.messages.PrivateRoomToggle;
import dev.slsk.messaging.messages.SetListenPortCommand;
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
import dev.slsk.network.tcp.Listener;
import dev.slsk.network.tcp.SocketListener;
import dev.slsk.options.BrowseOptions;
import dev.slsk.options.BrowseProgress;
import dev.slsk.options.ConnectionOptions;
import dev.slsk.options.SoulseekClientOptions;
import dev.slsk.options.SoulseekClientOptionsPatch;
import dev.slsk.search.DefaultSearchResponder;
import dev.slsk.search.SearchInternal;
import dev.slsk.search.SearchResponder;
import dev.slsk.search.SearchResponderClient;
import dev.slsk.transfer.TransferInternal;
import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
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

    /** Transfer orchestration, split out; see TransferEngine. */
    private final TransferEngine transfers;

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
        return unwrapped(transfers.getDownloadPlaceInQueue(username, filename));
    }

    @Override
    public Integer getDownloadPlaceInQueue(String username, String filename, CancellationSignal cancellationSignal) {
        return unwrapped(transfers.getDownloadPlaceInQueue(username, filename, cancellationSignal));
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
