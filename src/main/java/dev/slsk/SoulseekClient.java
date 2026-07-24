// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.common.Constants;
import dev.slsk.common.DefaultWaiter;
import dev.slsk.common.IOAdapter;
import dev.slsk.common.TokenBucket;
import dev.slsk.common.TokenFactory;
import dev.slsk.common.WaitKey;
import dev.slsk.common.Waiter;
import dev.slsk.diagnostics.DiagnosticEventListener;
import dev.slsk.diagnostics.DiagnosticFactory;
import dev.slsk.diagnostics.GlobalDiagnostic;
import dev.slsk.diagnostics.IDiagnosticFactory;
import dev.slsk.eventargs.BrowseProgressUpdatedEventArgs;
import dev.slsk.eventargs.DistributedChildEventArgs;
import dev.slsk.eventargs.DistributedParentEventArgs;
import dev.slsk.eventargs.DownloadDeniedEventArgs;
import dev.slsk.eventargs.DownloadFailedEventArgs;
import dev.slsk.eventargs.PrivateMessageReceivedEventArgs;
import dev.slsk.eventargs.PrivilegeNotificationReceivedEventArgs;
import dev.slsk.eventargs.PublicChatMessageReceivedEventArgs;
import dev.slsk.eventargs.RoomJoinedEventArgs;
import dev.slsk.eventargs.RoomLeftEventArgs;
import dev.slsk.eventargs.RoomMessageReceivedEventArgs;
import dev.slsk.eventargs.RoomTickerAddedEventArgs;
import dev.slsk.eventargs.RoomTickerListReceivedEventArgs;
import dev.slsk.eventargs.RoomTickerRemovedEventArgs;
import dev.slsk.eventargs.SearchRequestEventArgs;
import dev.slsk.eventargs.SearchRequestResponseEventArgs;
import dev.slsk.eventargs.SearchResponseReceivedEventArgs;
import dev.slsk.eventargs.SearchStateChangedEventArgs;
import dev.slsk.eventargs.SoulseekClientDisconnectedEventArgs;
import dev.slsk.eventargs.SoulseekClientStateChangedEventArgs;
import dev.slsk.eventargs.TransferProgressUpdatedEventArgs;
import dev.slsk.eventargs.TransferStateChangedEventArgs;
import dev.slsk.eventargs.UserCannotConnectEventArgs;
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
import dev.slsk.exceptions.RoomJoinForbiddenException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.TransferException;
import dev.slsk.exceptions.TransferNotFoundException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferReportedFailedException;
import dev.slsk.exceptions.TransferSizeMismatchException;
import dev.slsk.exceptions.TransferStreamException;
import dev.slsk.exceptions.UserEndPointCacheException;
import dev.slsk.exceptions.UserEndPointException;
import dev.slsk.exceptions.UserNotFoundException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.handlers.BrowseResponseConnection;
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
import dev.slsk.messaging.messages.AcknowledgePrivateMessageCommand;
import dev.slsk.messaging.messages.AcknowledgePrivilegeNotificationCommand;
import dev.slsk.messaging.messages.BrowseRequest;
import dev.slsk.messaging.messages.CheckPrivilegesRequest;
import dev.slsk.messaging.messages.FolderContentsRequest;
import dev.slsk.messaging.messages.GivePrivilegesCommand;
import dev.slsk.messaging.messages.JoinRoomRequest;
import dev.slsk.messaging.messages.LeaveRoomRequest;
import dev.slsk.messaging.messages.LoginRequest;
import dev.slsk.messaging.messages.LoginResponse;
import dev.slsk.messaging.messages.NewPassword;
import dev.slsk.messaging.messages.OutgoingMessage;
import dev.slsk.messaging.messages.PlaceInQueueRequest;
import dev.slsk.messaging.messages.PlaceInQueueResponse;
import dev.slsk.messaging.messages.PrivateMessageCommand;
import dev.slsk.messaging.messages.PrivateRoomAddOperator;
import dev.slsk.messaging.messages.PrivateRoomAddUser;
import dev.slsk.messaging.messages.PrivateRoomDropMembershipCommand;
import dev.slsk.messaging.messages.PrivateRoomDropOwnershipCommand;
import dev.slsk.messaging.messages.PrivateRoomRemoveOperator;
import dev.slsk.messaging.messages.PrivateRoomRemoveUser;
import dev.slsk.messaging.messages.PrivateRoomToggle;
import dev.slsk.messaging.messages.RoomListRequest;
import dev.slsk.messaging.messages.RoomMessageCommand;
import dev.slsk.messaging.messages.RoomSearchRequest;
import dev.slsk.messaging.messages.SearchRequest;
import dev.slsk.messaging.messages.SendUploadSpeedCommand;
import dev.slsk.messaging.messages.ServerPing;
import dev.slsk.messaging.messages.SetListenPortCommand;
import dev.slsk.messaging.messages.SetOnlineStatusCommand;
import dev.slsk.messaging.messages.SetRoomTickerCommand;
import dev.slsk.messaging.messages.SetSharedCountsCommand;
import dev.slsk.messaging.messages.StartPublicChatCommand;
import dev.slsk.messaging.messages.StopPublicChatCommand;
import dev.slsk.messaging.messages.TransferRequest;
import dev.slsk.messaging.messages.TransferResponse;
import dev.slsk.messaging.messages.UnwatchUserCommand;
import dev.slsk.messaging.messages.UploadDenied;
import dev.slsk.messaging.messages.UploadFailed;
import dev.slsk.messaging.messages.UserAddressRequest;
import dev.slsk.messaging.messages.UserAddressResponse;
import dev.slsk.messaging.messages.UserInfoRequest;
import dev.slsk.messaging.messages.UserPrivilegesRequest;
import dev.slsk.messaging.messages.UserSearchRequest;
import dev.slsk.messaging.messages.UserStatisticsRequest;
import dev.slsk.messaging.messages.UserStatusRequest;
import dev.slsk.messaging.messages.WatchUserRequest;
import dev.slsk.messaging.messages.WatchUserResponse;
import dev.slsk.messaging.messages.WishlistSearchRequest;
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
import dev.slsk.network.tcp.ConnectionDataEventArgs;
import dev.slsk.network.tcp.ConnectionDisconnectedEventArgs;
import dev.slsk.network.tcp.ConnectionEventListener;
import dev.slsk.network.tcp.Listener;
import dev.slsk.network.tcp.SocketListener;
import dev.slsk.options.BrowseOptions;
import dev.slsk.options.BrowseProgress;
import dev.slsk.options.ConnectionOptions;
import dev.slsk.options.DownloadStreamFactory;
import dev.slsk.options.PositionableInputStream;
import dev.slsk.options.PositionableOutputStream;
import dev.slsk.options.SearchOptions;
import dev.slsk.options.SearchResponseReceived;
import dev.slsk.options.SearchStateChange;
import dev.slsk.options.SoulseekClientOptions;
import dev.slsk.options.SoulseekClientOptionsPatch;
import dev.slsk.options.TransferOptions;
import dev.slsk.options.TransferProgressUpdate;
import dev.slsk.options.TransferStateChange;
import dev.slsk.options.UploadStreamFactory;
import dev.slsk.search.ISearchResponder;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * A client for the Soulseek file-sharing network.
 */
public class SoulseekClient
        implements ISoulseekClient,
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
    private final IOAdapter ioAdapter;
    private final TokenBucket uploadTokenBucket;
    private final TokenBucket downloadTokenBucket;
    private final ConnectionFactory connectionFactory;
    private final ListenerHandler listenerHandler;
    private final ISearchResponder searchResponder;
    private final PeerMessageHandler peerMessageHandler;
    private final DistributedMessageHandler distributedMessageHandler;
    private final PeerConnectionManager peerConnectionManager;
    private final DistributedConnectionManager distributedConnectionManager;
    private final ServerMessageHandler serverMessageHandler;
    private final IDiagnosticFactory diagnostic;
    private volatile ClientListenerFactory clientListenerFactory = SocketListener::new;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ScheduledExecutorService cleanupScheduler;
    private final Map<Event, CopyOnWriteArrayList<SoulseekClientEventListener<?>>> listeners =
            new EnumMap<>(Event.class);

    private volatile MessageConnection serverConnection;
    private volatile Listener listener;
    private volatile String address;
    private volatile InetSocketAddress ipEndPoint;
    private volatile String username;
    private volatile ServerInfo serverInfo = new ServerInfo();
    private volatile SoulseekClientStates state = SoulseekClientStates.DISCONNECTED;
    private volatile Map<Integer, TransferInternal> downloads = new ConcurrentHashMap<>();
    private volatile Map<Integer, TransferInternal> uploads = new ConcurrentHashMap<>();
    private volatile Map<Integer, SearchInternal> searches = new ConcurrentHashMap<>();
    private final Map<String, Boolean> uniqueKeys = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<InetSocketAddress>> endpointRequests = new ConcurrentHashMap<>();
    private final Map<String, Semaphore> uploadSemaphores = new ConcurrentHashMap<>();

    /** Creates a client with default options. */
    public SoulseekClient(int minorVersion) {
        this(minorVersion, null);
    }

    /** Creates a client. */
    public SoulseekClient(int minorVersion, SoulseekClientOptions options) {
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

    SoulseekClient(
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
            ISearchResponder searchResponder,
            Waiter waiter,
            TokenFactory tokenFactory,
            IDiagnosticFactory diagnosticFactory,
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
        this.waiter = waiter == null ? new DefaultWaiter(this.options.getMessageTimeout()) : waiter;
        this.tokenFactory = tokenFactory == null ? new TokenFactory(this.options.getStartingToken()) : tokenFactory;
        this.searchSemaphore = new Semaphore(this.options.getMaximumConcurrentSearches());
        this.globalDownloadSemaphore = new Semaphore(this.options.getMaximumConcurrentDownloads());
        this.globalUploadSemaphore = new Semaphore(this.options.getMaximumConcurrentUploads());
        this.ioAdapter = ioAdapter == null ? new IOAdapter() : ioAdapter;
        this.uploadTokenBucket = uploadTokenBucket == null
                ? new TokenBucket((this.options.getMaximumUploadSpeed() * 1024L) / 10, 100)
                : uploadTokenBucket;
        this.downloadTokenBucket = downloadTokenBucket == null
                ? new TokenBucket((this.options.getMaximumDownloadSpeed() * 1024L) / 10, 100)
                : downloadTokenBucket;
        this.connectionFactory = connectionFactory == null ? new DefaultConnectionFactory() : connectionFactory;
        for (Event event : Event.values()) {
            listeners.put(event, new CopyOnWriteArrayList<>());
        }

        diagnostic = diagnosticFactory == null
                ? new DiagnosticFactory(
                        this.options.getMinimumDiagnosticLevel(),
                        eventArgs -> raise(Event.DIAGNOSTIC_GENERATED, eventArgs))
                : diagnosticFactory;
        GlobalDiagnostic.init(diagnostic);

        this.listenerHandler = listenerHandler == null ? new DefaultListenerHandler(this) : listenerHandler;
        this.searchResponder = searchResponder == null ? new SearchResponder(this) : searchResponder;
        this.peerMessageHandler = peerMessageHandler == null ? new DefaultPeerMessageHandler(this) : peerMessageHandler;
        this.distributedMessageHandler = distributedMessageHandler == null
                ? new DefaultDistributedMessageHandler(this)
                : distributedMessageHandler;
        this.peerConnectionManager =
                peerConnectionManager == null ? new DefaultPeerConnectionManager(this) : peerConnectionManager;
        this.distributedConnectionManager = distributedConnectionManager == null
                ? new DefaultDistributedConnectionManager(this)
                : distributedConnectionManager;
        this.serverMessageHandler =
                serverMessageHandler == null ? new DefaultServerMessageHandler(this) : serverMessageHandler;

        bindEvents();
        cleanupScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "soulseek-client-cleanup");
            thread.setDaemon(true);
            return thread;
        });
        cleanupScheduler.scheduleAtFixedRate(() -> cleanupUploadSemaphoresAsync(), 15, 15, TimeUnit.MINUTES);
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
                        .map(peer -> new DistributedPeer(peer.username(), peer.ipEndPoint()))
                        .toList();
        DistributedPeer parentSnapshot = parent == null
                ? new DistributedPeer("", null)
                : new DistributedPeer(parent.username(), parent.ipEndPoint());
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
        return ipEndPoint == null ? null : ipEndPoint.getAddress();
    }

    /** Returns the connected server endpoint, or {@code null}. */
    public final InetSocketAddress getIpEndPoint() {
        return ipEndPoint;
    }

    /** Returns the configured client options. */
    @Override
    public final SoulseekClientOptions getOptions() {
        return options;
    }

    /** Returns the connected server port, or {@code null}. */
    public final Integer getPort() {
        return ipEndPoint == null ? null : ipEndPoint.getPort();
    }

    /** Returns the accumulated server information. */
    public final ServerInfo getServerInfo() {
        return serverInfo;
    }

    /** Returns current client state. */
    @Override
    public final SoulseekClientStates getState() {
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

    public final void addBrowseProgressUpdatedListener(
            SoulseekClientEventListener<BrowseProgressUpdatedEventArgs> value) {
        add(Event.BROWSE_PROGRESS_UPDATED, value);
    }

    public final void removeBrowseProgressUpdatedListener(
            SoulseekClientEventListener<BrowseProgressUpdatedEventArgs> value) {
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

    public final void addDisconnectedListener(SoulseekClientEventListener<SoulseekClientDisconnectedEventArgs> value) {
        add(Event.DISCONNECTED, value);
    }

    public final void removeDisconnectedListener(
            SoulseekClientEventListener<SoulseekClientDisconnectedEventArgs> value) {
        remove(Event.DISCONNECTED, value);
    }

    public final void addDistributedChildAddedListener(SoulseekClientEventListener<DistributedChildEventArgs> value) {
        add(Event.DISTRIBUTED_CHILD_ADDED, value);
    }

    public final void removeDistributedChildAddedListener(
            SoulseekClientEventListener<DistributedChildEventArgs> value) {
        remove(Event.DISTRIBUTED_CHILD_ADDED, value);
    }

    public final void addDistributedChildDisconnectedListener(
            SoulseekClientEventListener<DistributedChildEventArgs> value) {
        add(Event.DISTRIBUTED_CHILD_DISCONNECTED, value);
    }

    public final void removeDistributedChildDisconnectedListener(
            SoulseekClientEventListener<DistributedChildEventArgs> value) {
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

    public final void addDistributedParentAdoptedListener(
            SoulseekClientEventListener<DistributedParentEventArgs> value) {
        add(Event.DISTRIBUTED_PARENT_ADOPTED, value);
    }

    public final void removeDistributedParentAdoptedListener(
            SoulseekClientEventListener<DistributedParentEventArgs> value) {
        remove(Event.DISTRIBUTED_PARENT_ADOPTED, value);
    }

    public final void addDistributedParentDisconnectedListener(
            SoulseekClientEventListener<DistributedParentEventArgs> value) {
        add(Event.DISTRIBUTED_PARENT_DISCONNECTED, value);
    }

    public final void removeDistributedParentDisconnectedListener(
            SoulseekClientEventListener<DistributedParentEventArgs> value) {
        remove(Event.DISTRIBUTED_PARENT_DISCONNECTED, value);
    }

    public final void addDownloadDeniedListener(SoulseekClientEventListener<DownloadDeniedEventArgs> value) {
        add(Event.DOWNLOAD_DENIED, value);
    }

    public final void removeDownloadDeniedListener(SoulseekClientEventListener<DownloadDeniedEventArgs> value) {
        remove(Event.DOWNLOAD_DENIED, value);
    }

    public final void addDownloadFailedListener(SoulseekClientEventListener<DownloadFailedEventArgs> value) {
        add(Event.DOWNLOAD_FAILED, value);
    }

    public final void removeDownloadFailedListener(SoulseekClientEventListener<DownloadFailedEventArgs> value) {
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
            SoulseekClientEventListener<PrivateMessageReceivedEventArgs> value) {
        add(Event.PRIVATE_MESSAGE_RECEIVED, value);
    }

    public final void removePrivateMessageReceivedListener(
            SoulseekClientEventListener<PrivateMessageReceivedEventArgs> value) {
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
            SoulseekClientEventListener<PrivilegeNotificationReceivedEventArgs> value) {
        add(Event.PRIVILEGE_NOTIFICATION_RECEIVED, value);
    }

    public final void removePrivilegeNotificationReceivedListener(
            SoulseekClientEventListener<PrivilegeNotificationReceivedEventArgs> value) {
        remove(Event.PRIVILEGE_NOTIFICATION_RECEIVED, value);
    }

    public final void addPromotedToDistributedBranchRootListener(SoulseekClientEventListener<Void> value) {
        add(Event.PROMOTED_TO_DISTRIBUTED_BRANCH_ROOT, value);
    }

    public final void removePromotedToDistributedBranchRootListener(SoulseekClientEventListener<Void> value) {
        remove(Event.PROMOTED_TO_DISTRIBUTED_BRANCH_ROOT, value);
    }

    public final void addPublicChatMessageReceivedListener(
            SoulseekClientEventListener<PublicChatMessageReceivedEventArgs> value) {
        add(Event.PUBLIC_CHAT_MESSAGE_RECEIVED, value);
    }

    public final void removePublicChatMessageReceivedListener(
            SoulseekClientEventListener<PublicChatMessageReceivedEventArgs> value) {
        remove(Event.PUBLIC_CHAT_MESSAGE_RECEIVED, value);
    }

    public final void addRoomJoinedListener(SoulseekClientEventListener<RoomJoinedEventArgs> value) {
        add(Event.ROOM_JOINED, value);
    }

    public final void removeRoomJoinedListener(SoulseekClientEventListener<RoomJoinedEventArgs> value) {
        remove(Event.ROOM_JOINED, value);
    }

    public final void addRoomLeftListener(SoulseekClientEventListener<RoomLeftEventArgs> value) {
        add(Event.ROOM_LEFT, value);
    }

    public final void removeRoomLeftListener(SoulseekClientEventListener<RoomLeftEventArgs> value) {
        remove(Event.ROOM_LEFT, value);
    }

    public final void addRoomListReceivedListener(SoulseekClientEventListener<RoomList> value) {
        add(Event.ROOM_LIST_RECEIVED, value);
    }

    public final void removeRoomListReceivedListener(SoulseekClientEventListener<RoomList> value) {
        remove(Event.ROOM_LIST_RECEIVED, value);
    }

    public final void addRoomMessageReceivedListener(SoulseekClientEventListener<RoomMessageReceivedEventArgs> value) {
        add(Event.ROOM_MESSAGE_RECEIVED, value);
    }

    public final void removeRoomMessageReceivedListener(
            SoulseekClientEventListener<RoomMessageReceivedEventArgs> value) {
        remove(Event.ROOM_MESSAGE_RECEIVED, value);
    }

    public final void addRoomTickerAddedListener(SoulseekClientEventListener<RoomTickerAddedEventArgs> value) {
        add(Event.ROOM_TICKER_ADDED, value);
    }

    public final void removeRoomTickerAddedListener(SoulseekClientEventListener<RoomTickerAddedEventArgs> value) {
        remove(Event.ROOM_TICKER_ADDED, value);
    }

    public final void addRoomTickerListReceivedListener(
            SoulseekClientEventListener<RoomTickerListReceivedEventArgs> value) {
        add(Event.ROOM_TICKER_LIST_RECEIVED, value);
    }

    public final void removeRoomTickerListReceivedListener(
            SoulseekClientEventListener<RoomTickerListReceivedEventArgs> value) {
        remove(Event.ROOM_TICKER_LIST_RECEIVED, value);
    }

    public final void addRoomTickerRemovedListener(SoulseekClientEventListener<RoomTickerRemovedEventArgs> value) {
        add(Event.ROOM_TICKER_REMOVED, value);
    }

    public final void removeRoomTickerRemovedListener(SoulseekClientEventListener<RoomTickerRemovedEventArgs> value) {
        remove(Event.ROOM_TICKER_REMOVED, value);
    }

    public final void addSearchRequestReceivedListener(SoulseekClientEventListener<SearchRequestEventArgs> value) {
        add(Event.SEARCH_REQUEST_RECEIVED, value);
    }

    public final void removeSearchRequestReceivedListener(SoulseekClientEventListener<SearchRequestEventArgs> value) {
        remove(Event.SEARCH_REQUEST_RECEIVED, value);
    }

    public final void addSearchResponseDeliveredListener(
            SoulseekClientEventListener<SearchRequestResponseEventArgs> value) {
        add(Event.SEARCH_RESPONSE_DELIVERED, value);
    }

    public final void removeSearchResponseDeliveredListener(
            SoulseekClientEventListener<SearchRequestResponseEventArgs> value) {
        remove(Event.SEARCH_RESPONSE_DELIVERED, value);
    }

    public final void addSearchResponseDeliveryFailedListener(
            SoulseekClientEventListener<SearchRequestResponseEventArgs> value) {
        add(Event.SEARCH_RESPONSE_DELIVERY_FAILED, value);
    }

    public final void removeSearchResponseDeliveryFailedListener(
            SoulseekClientEventListener<SearchRequestResponseEventArgs> value) {
        remove(Event.SEARCH_RESPONSE_DELIVERY_FAILED, value);
    }

    public final void addSearchResponseReceivedListener(
            SoulseekClientEventListener<SearchResponseReceivedEventArgs> value) {
        add(Event.SEARCH_RESPONSE_RECEIVED, value);
    }

    public final void removeSearchResponseReceivedListener(
            SoulseekClientEventListener<SearchResponseReceivedEventArgs> value) {
        remove(Event.SEARCH_RESPONSE_RECEIVED, value);
    }

    public final void addSearchStateChangedListener(SoulseekClientEventListener<SearchStateChangedEventArgs> value) {
        add(Event.SEARCH_STATE_CHANGED, value);
    }

    public final void removeSearchStateChangedListener(SoulseekClientEventListener<SearchStateChangedEventArgs> value) {
        remove(Event.SEARCH_STATE_CHANGED, value);
    }

    public final void addServerInfoReceivedListener(SoulseekClientEventListener<ServerInfo> value) {
        add(Event.SERVER_INFO_RECEIVED, value);
    }

    public final void removeServerInfoReceivedListener(SoulseekClientEventListener<ServerInfo> value) {
        remove(Event.SERVER_INFO_RECEIVED, value);
    }

    public final void addStateChangedListener(SoulseekClientEventListener<SoulseekClientStateChangedEventArgs> value) {
        add(Event.STATE_CHANGED, value);
    }

    public final void removeStateChangedListener(
            SoulseekClientEventListener<SoulseekClientStateChangedEventArgs> value) {
        remove(Event.STATE_CHANGED, value);
    }

    public final void addTransferProgressUpdatedListener(
            SoulseekClientEventListener<TransferProgressUpdatedEventArgs> value) {
        add(Event.TRANSFER_PROGRESS_UPDATED, value);
    }

    public final void removeTransferProgressUpdatedListener(
            SoulseekClientEventListener<TransferProgressUpdatedEventArgs> value) {
        remove(Event.TRANSFER_PROGRESS_UPDATED, value);
    }

    public final void addTransferStateChangedListener(
            SoulseekClientEventListener<TransferStateChangedEventArgs> value) {
        add(Event.TRANSFER_STATE_CHANGED, value);
    }

    public final void removeTransferStateChangedListener(
            SoulseekClientEventListener<TransferStateChangedEventArgs> value) {
        remove(Event.TRANSFER_STATE_CHANGED, value);
    }

    public final void addUserCannotConnectListener(SoulseekClientEventListener<UserCannotConnectEventArgs> value) {
        add(Event.USER_CANNOT_CONNECT, value);
    }

    public final void removeUserCannotConnectListener(SoulseekClientEventListener<UserCannotConnectEventArgs> value) {
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

    public CompletableFuture<Void> sendPrivateMessageAsync(String requestedUsername, String message) {
        return sendPrivateMessageAsync(requestedUsername, message, CancellationToken.none());
    }

    public CompletableFuture<Void> sendPrivateMessageAsync(
            String requestedUsername, String message, CancellationToken cancellationToken) {
        requireText(requestedUsername, "username");
        requireNonEmpty(message, "message");
        requireLoggedIn("send a private message");
        return writeServerAsync(
                new PrivateMessageCommand(requestedUsername, message),
                cancellationToken,
                "Failed to send private message to user " + requestedUsername + ": ");
    }

    public CompletableFuture<Void> addPrivateRoomMemberAsync(String roomName, String requestedUsername) {
        return addPrivateRoomMemberAsync(roomName, requestedUsername, CancellationToken.none());
    }

    public CompletableFuture<Void> addPrivateRoomMemberAsync(
            String roomName, String requestedUsername, CancellationToken cancellationToken) {
        requireText(roomName, "roomName");
        requireText(requestedUsername, "username");
        requireLoggedIn("add members to private rooms");
        return executeCorrelatedServerCommand(
                new PrivateRoomAddUser(roomName, requestedUsername),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_ADD_USER, roomName, requestedUsername),
                cancellationToken,
                "Failed to add user " + requestedUsername + " as member of private room " + roomName + ": ");
    }

    public CompletableFuture<Void> changePasswordAsync(String password) {
        return changePasswordAsync(password, CancellationToken.none());
    }

    public CompletableFuture<Void> changePasswordAsync(String password, CancellationToken cancellationToken) {
        requireText(password, "password");
        requireLoggedIn("change a password");
        return executeCorrelatedServerRequest(
                        new NewPassword(password),
                        new WaitKey(MessageCode.Server.NEW_PASSWORD),
                        String.class,
                        cancellationToken,
                        "Failed to change password: ")
                .thenApply(response -> {
                    if (!password.equals(response)) {
                        throw new SoulseekClientException("Probably failed to change password; the response "
                                + "from the server doesn't match the specified "
                                + "password");
                    }
                    return null;
                });
    }

    /**
     * Connects to the default Soulseek server and logs in.
     *
     * @param requestedUsername the login username
     * @param password the login password
     * @return the connection operation
     */
    public CompletableFuture<Void> connectAsync(String requestedUsername, String password) {
        return connectAsync(DEFAULT_ADDRESS, DEFAULT_PORT, requestedUsername, password, CancellationToken.none());
    }

    /**
     * Connects to the default Soulseek server and logs in.
     *
     * @param requestedUsername the login username
     * @param password the login password
     * @param cancellationToken the cancellation token
     * @return the connection operation
     */
    public CompletableFuture<Void> connectAsync(
            String requestedUsername, String password, CancellationToken cancellationToken) {
        return connectAsync(DEFAULT_ADDRESS, DEFAULT_PORT, requestedUsername, password, cancellationToken);
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
    public CompletableFuture<Void> connectAsync(
            String requestedAddress, int requestedPort, String requestedUsername, String password) {
        return connectAsync(requestedAddress, requestedPort, requestedUsername, password, CancellationToken.none());
    }

    /**
     * Connects to a Soulseek server and logs in.
     *
     * @param requestedAddress the server address
     * @param requestedPort the server port
     * @param requestedUsername the login username
     * @param password the login password
     * @param cancellationToken the cancellation token
     * @return the connection operation
     */
    public CompletableFuture<Void> connectAsync(
            String requestedAddress,
            int requestedPort,
            String requestedUsername,
            String password,
            CancellationToken cancellationToken) {
        requireText(requestedAddress, "address");
        if (requestedPort < 0 || requestedPort > 65_535) {
            throw new IllegalArgumentException("port must be between 0 and 65535 (specified: " + requestedPort + ")");
        }
        requireNonEmpty(requestedUsername, "username");
        requireNonEmpty(password, "password");
        if (state.hasFlag(SoulseekClientStates.CONNECTING) || state.hasFlag(SoulseekClientStates.LOGGING_IN)) {
            throw new IllegalStateException("A connection is already in the process of " + "being established");
        }
        if (state.hasFlag(SoulseekClientStates.CONNECTED)) {
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
                        options.getListenIPAddress(), options.getListenPort(), options.getIncomingConnectionOptions());
                probe.start();
            } catch (Throwable failure) {
                throw new ListenException("Failed to start listening on "
                        + options.getListenIPAddress() + ":"
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
                defaultToken(cancellationToken));
    }

    public CompletableFuture<BrowseResponse> browseAsync(String requestedUsername) {
        return browseAsync(requestedUsername, null, CancellationToken.none());
    }

    public CompletableFuture<BrowseResponse> browseAsync(String requestedUsername, BrowseOptions browseOptions) {
        return browseAsync(requestedUsername, browseOptions, CancellationToken.none());
    }

    public CompletableFuture<BrowseResponse> browseAsync(
            String requestedUsername, CancellationToken cancellationToken) {
        return browseAsync(requestedUsername, null, cancellationToken);
    }

    public CompletableFuture<BrowseResponse> browseAsync(
            String requestedUsername, BrowseOptions browseOptions, CancellationToken cancellationToken) {
        requireText(requestedUsername, "username");
        requireLoggedIn("browse");
        BrowseOptions operationOptions = browseOptions == null ? new BrowseOptions() : browseOptions;
        CancellationToken token = defaultToken(cancellationToken);
        WaitKey browseWaitKey = new WaitKey(MessageCode.Peer.BROWSE_RESPONSE, requestedUsername);
        CompletableFuture<BrowseResponse> browseWait;
        CompletableFuture<BrowseResponseConnection> connectionWait;
        try {
            browseWait = waiter.waitIndefinitelyAsync(browseWaitKey, BrowseResponse.class, token);
            connectionWait = waiter.waitAsync(
                    new WaitKey(Constants.WaitKey.BROWSE_RESPONSE_CONNECTION, requestedUsername),
                    BrowseResponseConnection.class,
                    operationOptions.getResponseTimeout(),
                    token);
        } catch (Throwable failure) {
            return mapClientFailure(
                    CompletableFuture.failedFuture(failure),
                    "Failed to browse user " + requestedUsername + ": ",
                    UserOfflineException.class);
        }

        CompletableFuture<BrowseResponseConnection> setup = getUserEndPointAsync(requestedUsername, token)
                .thenCompose(endpoint ->
                        peerConnectionManager.getOrAddMessageConnectionAsync(requestedUsername, endpoint, token))
                .thenCompose(connection -> invokeMessageWrite(connection, new BrowseRequest(), token))
                .thenCompose(ignored -> connectionWait);
        CompletableFuture<BrowseResponse> operation = setup.handle((responseConnection, failure) -> {
                    if (failure == null) {
                        return responseConnection;
                    }
                    Throwable cause = unwrap(failure);
                    waiter.fail(browseWaitKey, cause);
                    throw new CompletionException(cause);
                })
                .thenCompose(responseConnection -> {
                    MessageConnection connection = responseConnection.connection();
                    long responseLength = responseConnection.eventArgs().getLength() - 4;
                    AtomicBoolean completionEventFired = new AtomicBoolean();
                    dev.slsk.network.MessageConnectionEventListener<dev.slsk.network.MessageDataEventArgs>
                            progressListener = (sender, eventArgs) -> updateBrowseProgress(
                            requestedUsername,
                            operationOptions,
                            eventArgs.getCurrentLength(),
                            eventArgs.getTotalLength(),
                            completionEventFired);
                    connection.addDisconnectedListener((sender, eventArgs) -> waiter.fail(
                            browseWaitKey,
                            new ConnectionException(
                                    "Peer connection disconnected " + "unexpectedly: " + eventArgs.getMessage(),
                                    eventArgs.getException())));
                    connection.addMessageDataReadListener(progressListener);
                    updateBrowseProgress(requestedUsername, operationOptions, 0, responseLength, completionEventFired);
                    return browseWait.thenApply(response -> {
                        connection.removeMessageDataReadListener(progressListener);
                        if (!completionEventFired.get()) {
                            updateBrowseProgress(
                                    requestedUsername,
                                    operationOptions,
                                    responseLength,
                                    responseLength,
                                    completionEventFired);
                        }
                        return response;
                    });
                });
        return mapClientFailure(
                operation, "Failed to browse user " + requestedUsername + ": ", UserOfflineException.class);
    }

    public CompletableFuture<Void> connectToUserAsync(String requestedUsername) {
        return connectToUserAsync(requestedUsername, false, CancellationToken.none());
    }

    public CompletableFuture<Void> connectToUserAsync(String requestedUsername, boolean invalidateCache) {
        return connectToUserAsync(requestedUsername, invalidateCache, CancellationToken.none());
    }

    public CompletableFuture<Void> connectToUserAsync(String requestedUsername, CancellationToken cancellationToken) {
        return connectToUserAsync(requestedUsername, false, cancellationToken);
    }

    public CompletableFuture<Void> connectToUserAsync(
            String requestedUsername, boolean invalidateCache, CancellationToken cancellationToken) {
        requireText(requestedUsername, "username");
        requireLoggedIn("connect to other users");
        CancellationToken token = defaultToken(cancellationToken);
        CompletableFuture<Void> operation = getUserEndPointAsync(requestedUsername, token)
                .thenCompose(endpoint -> {
                    if (invalidateCache
                            && peerConnectionManager.tryInvalidateMessageConnectionCache(requestedUsername)) {
                        diagnostic.debug("Invalidated message connection cache for " + requestedUsername);
                    }
                    return peerConnectionManager
                            .getOrAddMessageConnectionAsync(requestedUsername, endpoint, token)
                            .thenApply(ignored -> null);
                });
        return mapClientFailure(
                operation, "Failed to connect to user " + requestedUsername + ": ", UserOfflineException.class);
    }

    public CompletableFuture<Integer> getPrivilegesAsync() {
        return getPrivilegesAsync(CancellationToken.none());
    }

    public CompletableFuture<Integer> getPrivilegesAsync(CancellationToken cancellationToken) {
        requireLoggedIn("check privileges");
        return executeCorrelatedServerRequest(
                new CheckPrivilegesRequest(),
                new WaitKey(MessageCode.Server.CHECK_PRIVILEGES),
                Integer.class,
                cancellationToken,
                "Failed to get privileges: ");
    }

    public CompletableFuture<RoomList> getRoomListAsync() {
        return getRoomListAsync(CancellationToken.none());
    }

    public CompletableFuture<RoomList> getRoomListAsync(CancellationToken cancellationToken) {
        requireLoggedIn("fetch the list of chat rooms");
        return executeCorrelatedServerRequest(
                new RoomListRequest(),
                new WaitKey(MessageCode.Server.ROOM_LIST),
                RoomList.class,
                cancellationToken,
                "Failed to fetch the list of chat rooms from the server: ");
    }

    public CompletableFuture<List<Directory>> getDirectoryContentsAsync(
            String requestedUsername, String directoryName) {
        return getDirectoryContentsAsync(requestedUsername, directoryName, null, CancellationToken.none());
    }

    public CompletableFuture<List<Directory>> getDirectoryContentsAsync(
            String requestedUsername, String directoryName, int operationToken) {
        return getDirectoryContentsAsync(requestedUsername, directoryName, operationToken, CancellationToken.none());
    }

    public CompletableFuture<List<Directory>> getDirectoryContentsAsync(
            String requestedUsername, String directoryName, CancellationToken cancellationToken) {
        return getDirectoryContentsAsync(requestedUsername, directoryName, null, cancellationToken);
    }

    public CompletableFuture<List<Directory>> getDirectoryContentsAsync(
            String requestedUsername,
            String directoryName,
            Integer operationToken,
            CancellationToken cancellationToken) {
        requireText(requestedUsername, "username");
        requireText(directoryName, "directoryName");
        requireLoggedIn("fetch directory contents");
        int tokenValue = operationToken == null ? getNextToken() : operationToken;
        CancellationToken token = defaultToken(cancellationToken);
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
        CompletableFuture<List<Directory>> operation = getUserEndPointAsync(requestedUsername, token)
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

    public CompletableFuture<Integer> getDownloadPlaceInQueueAsync(String requestedUsername, String filename) {
        return getDownloadPlaceInQueueAsync(requestedUsername, filename, CancellationToken.none());
    }

    public CompletableFuture<Integer> getDownloadPlaceInQueueAsync(
            String requestedUsername, String filename, CancellationToken cancellationToken) {
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
        CancellationToken token = defaultToken(cancellationToken);
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
        CompletableFuture<Integer> operation = getUserEndPointAsync(requestedUsername, token)
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

    public CompletableFuture<RoomData> joinRoomAsync(String roomName) {
        return joinRoomAsync(roomName, false, CancellationToken.none());
    }

    public CompletableFuture<RoomData> joinRoomAsync(String roomName, boolean isPrivate) {
        return joinRoomAsync(roomName, isPrivate, CancellationToken.none());
    }

    public CompletableFuture<RoomData> joinRoomAsync(String roomName, CancellationToken cancellationToken) {
        return joinRoomAsync(roomName, false, cancellationToken);
    }

    public CompletableFuture<RoomData> joinRoomAsync(
            String roomName, boolean isPrivate, CancellationToken cancellationToken) {
        requireText(roomName, "roomName");
        requireLoggedIn("join a chat room");
        CancellationToken token = defaultToken(cancellationToken);
        CompletableFuture<RoomData> wait;
        try {
            wait = waiter.waitAsync(new WaitKey(MessageCode.Server.JOIN_ROOM, roomName), RoomData.class, null, token);
        } catch (Throwable failure) {
            return mapClientFailure(
                    CompletableFuture.failedFuture(failure),
                    "Failed to join chat room " + roomName + ": ",
                    RoomJoinForbiddenException.class,
                    NoResponseException.class);
        }
        CompletableFuture<RoomData> translatedWait = wait.handle((response, failure) -> {
            if (failure == null) {
                return response;
            }
            Throwable cause = unwrap(failure);
            if (cause instanceof TimeoutException) {
                throw new CompletionException(new NoResponseException("The server didn't respond to the request "
                        + "to join chat room " + roomName
                        + ". This probably indicates that the "
                        + "room is already joined."));
            }
            throw new CompletionException(cause);
        });
        CompletableFuture<RoomData> operation = invokeServerWrite(new JoinRoomRequest(roomName, isPrivate), token)
                .thenCompose(ignored -> translatedWait);
        return mapClientFailure(
                operation,
                "Failed to join chat room " + roomName + ": ",
                RoomJoinForbiddenException.class,
                NoResponseException.class);
    }

    public CompletableFuture<Void> leaveRoomAsync(String roomName) {
        return leaveRoomAsync(roomName, CancellationToken.none());
    }

    public CompletableFuture<Void> leaveRoomAsync(String roomName, CancellationToken cancellationToken) {
        requireText(roomName, "roomName");
        requireLoggedIn("leave a chat room");
        CancellationToken token = defaultToken(cancellationToken);
        CompletableFuture<Void> wait;
        try {
            wait = waiter.waitAsync(new WaitKey(MessageCode.Server.LEAVE_ROOM, roomName), null, token);
        } catch (Throwable failure) {
            return mapClientFailure(
                    CompletableFuture.failedFuture(failure),
                    "Failed to leave chat room " + roomName + ": ",
                    NoResponseException.class);
        }
        CompletableFuture<Void> translatedWait = wait.handle((ignored, failure) -> {
            if (failure == null) {
                return null;
            }
            Throwable cause = unwrap(failure);
            if (cause instanceof TimeoutException) {
                throw new CompletionException(new NoResponseException("The server didn't respond to the request "
                        + "to leave chat room " + roomName
                        + ".  This probably indicates that the "
                        + "room is not joined."));
            }
            throw new CompletionException(cause);
        });
        CompletableFuture<Void> operation =
                invokeServerWrite(new LeaveRoomRequest(roomName), token).thenCompose(ignored -> translatedWait);
        return mapClientFailure(operation, "Failed to leave chat room " + roomName + ": ", NoResponseException.class);
    }

    public CompletableFuture<Boolean> getUserPrivilegedAsync(String requestedUsername) {
        return getUserPrivilegedAsync(requestedUsername, CancellationToken.none());
    }

    public CompletableFuture<Boolean> getUserPrivilegedAsync(
            String requestedUsername, CancellationToken cancellationToken) {
        requireText(requestedUsername, "username");
        requireLoggedIn("check user privileges");
        return executeCorrelatedServerRequest(
                new UserPrivilegesRequest(requestedUsername),
                new WaitKey(MessageCode.Server.USER_PRIVILEGES, requestedUsername),
                Boolean.class,
                cancellationToken,
                "Failed to get privileges for " + requestedUsername + ": ",
                UserOfflineException.class);
    }

    public CompletableFuture<UserStatistics> getUserStatisticsAsync(String requestedUsername) {
        return getUserStatisticsAsync(requestedUsername, CancellationToken.none());
    }

    public CompletableFuture<UserStatistics> getUserStatisticsAsync(
            String requestedUsername, CancellationToken cancellationToken) {
        requireText(requestedUsername, "username");
        requireLoggedIn("fetch user statistics");
        return executeCorrelatedServerRequest(
                new UserStatisticsRequest(requestedUsername),
                new WaitKey(MessageCode.Server.GET_USER_STATS, requestedUsername),
                UserStatistics.class,
                cancellationToken,
                "Failed to retrieve statistics for user " + username + ": ");
    }

    public CompletableFuture<UserInfo> getUserInfoAsync(String requestedUsername) {
        return getUserInfoAsync(requestedUsername, CancellationToken.none());
    }

    public CompletableFuture<UserInfo> getUserInfoAsync(String requestedUsername, CancellationToken cancellationToken) {
        requireText(requestedUsername, "username");
        requireLoggedIn("fetch user information");
        CancellationToken token = defaultToken(cancellationToken);
        CompletableFuture<UserInfo> infoWait;
        try {
            infoWait = waiter.waitAsync(
                    new WaitKey(MessageCode.Peer.INFO_RESPONSE, requestedUsername), UserInfo.class, null, token);
        } catch (Throwable failure) {
            return mapClientFailure(
                    CompletableFuture.failedFuture(failure),
                    "Failed to retrieve information for user " + requestedUsername + ": ",
                    UserOfflineException.class);
        }
        CompletableFuture<UserInfo> operation = getUserEndPointAsync(requestedUsername, token)
                .thenCompose(endpoint ->
                        peerConnectionManager.getOrAddMessageConnectionAsync(requestedUsername, endpoint, token))
                .thenCompose(connection -> invokeMessageWrite(connection, new UserInfoRequest(), token))
                .thenCompose(ignored -> infoWait);
        return mapClientFailure(
                operation,
                "Failed to retrieve information for user " + requestedUsername + ": ",
                UserOfflineException.class);
    }

    public CompletableFuture<UserStatus> getUserStatusAsync(String requestedUsername) {
        return getUserStatusAsync(requestedUsername, CancellationToken.none());
    }

    public CompletableFuture<UserStatus> getUserStatusAsync(
            String requestedUsername, CancellationToken cancellationToken) {
        requireText(requestedUsername, "username");
        requireLoggedIn("fetch user status");
        return executeCorrelatedServerRequest(
                new UserStatusRequest(requestedUsername),
                new WaitKey(MessageCode.Server.GET_STATUS, requestedUsername),
                UserStatus.class,
                cancellationToken,
                "Failed to retrieve status for user " + username + ": ",
                UserOfflineException.class);
    }

    public CompletableFuture<UserData> watchUserAsync(String requestedUsername) {
        return watchUserAsync(requestedUsername, CancellationToken.none());
    }

    public CompletableFuture<UserData> watchUserAsync(String requestedUsername, CancellationToken cancellationToken) {
        requireText(requestedUsername, "username");
        requireLoggedIn("add users");
        return executeCorrelatedServerRequest(
                        new WatchUserRequest(requestedUsername),
                        new WaitKey(MessageCode.Server.WATCH_USER, requestedUsername),
                        WatchUserResponse.class,
                        cancellationToken,
                        "Failed to watch user " + requestedUsername + ": ",
                        UserNotFoundException.class)
                .thenApply(response -> {
                    if (!response.isExists()) {
                        throw new UserNotFoundException("User " + requestedUsername + " does not exist");
                    }
                    return response.getUserData();
                });
    }

    public CompletableFuture<Void> grantUserPrivilegesAsync(String requestedUsername, int days) {
        return grantUserPrivilegesAsync(requestedUsername, days, CancellationToken.none());
    }

    public CompletableFuture<Void> grantUserPrivilegesAsync(
            String requestedUsername, int days, CancellationToken cancellationToken) {
        requireText(requestedUsername, "username");
        if (days <= 0) {
            throw new IllegalArgumentException("The number of days granted must be greater than zero");
        }
        requireLoggedIn("grant user privileges");
        return writeServerAsync(
                new GivePrivilegesCommand(requestedUsername, days),
                cancellationToken,
                "Failed to grant " + days + " days of privileges to " + requestedUsername + ": ");
    }

    public CompletableFuture<Long> pingServerAsync() {
        return pingServerAsync(CancellationToken.none());
    }

    public CompletableFuture<Long> pingServerAsync(CancellationToken cancellationToken) {
        requireLoggedIn("send a ping");
        CancellationToken token = defaultToken(cancellationToken);
        CompletableFuture<Void> wait;
        try {
            wait = waiter.waitAsync(new WaitKey(MessageCode.Server.PING), null, token);
        } catch (Throwable failure) {
            return mapClientFailure(CompletableFuture.failedFuture(failure), "Failed to ping the server: ");
        }
        long started = System.nanoTime();
        CompletableFuture<Void> responseWait = wait;
        CompletableFuture<Long> operation = invokeServerWrite(new ServerPing(), token)
                .thenCompose(ignored -> responseWait)
                .thenApply(ignored -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
        return mapClientFailure(operation, "Failed to ping the server: ");
    }

    /**
     * Applies a patch to the current client options.
     *
     * @param patch the option substitutions
     * @return whether reconnecting is required for full effect
     */
    public CompletableFuture<Boolean> reconfigureOptionsAsync(SoulseekClientOptionsPatch patch) {
        return reconfigureOptionsAsync(patch, CancellationToken.none());
    }

    /**
     * Applies a patch to the current client options.
     *
     * @param patch the option substitutions
     * @param cancellationToken the cancellation token
     * @return whether reconnecting is required for full effect
     */
    public CompletableFuture<Boolean> reconfigureOptionsAsync(
            SoulseekClientOptionsPatch patch, CancellationToken cancellationToken) {
        Objects.requireNonNull(patch, "patch");
        boolean addressChanged = patch.getListenIPAddress() != null
                && !patch.getListenIPAddress().equals(options.getListenIPAddress());
        boolean portChanged = patch.getListenPort() != null && patch.getListenPort() != options.getListenPort();
        if (addressChanged || portChanged) {
            InetAddress newAddress =
                    patch.getListenIPAddress() == null ? options.getListenIPAddress() : patch.getListenIPAddress();
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
        return reconfigureOptionsInternalAsync(patch, defaultToken(cancellationToken));
    }

    /** Downloads a remote file to a local file. */
    public CompletableFuture<Transfer> downloadAsync(
            String requestedUsername, String remoteFilename, String localFilename) {
        return downloadAsync(
                requestedUsername, remoteFilename, localFilename, null, 0, null, null, CancellationToken.none());
    }

    /** Downloads a remote file with an expected size. */
    public CompletableFuture<Transfer> downloadAsync(
            String requestedUsername, String remoteFilename, String localFilename, Long size) {
        return downloadAsync(
                requestedUsername, remoteFilename, localFilename, size, 0, null, null, CancellationToken.none());
    }

    /** Downloads a remote file with cancellation. */
    public CompletableFuture<Transfer> downloadAsync(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            CancellationToken cancellationToken) {
        return downloadAsync(requestedUsername, remoteFilename, localFilename, null, 0, null, null, cancellationToken);
    }

    /** Downloads a remote file from a resume offset. */
    public CompletableFuture<Transfer> downloadAsync(
            String requestedUsername, String remoteFilename, String localFilename, Long size, long startOffset) {
        return downloadAsync(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                null,
                null,
                CancellationToken.none());
    }

    /** Downloads a remote file with a specific token. */
    public CompletableFuture<Transfer> downloadAsync(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token) {
        return downloadAsync(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                token,
                null,
                CancellationToken.none());
    }

    /** Downloads a remote file using supplied transfer options. */
    public CompletableFuture<Transfer> downloadAsync(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions) {
        return downloadAsync(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                token,
                transferOptions,
                CancellationToken.none());
    }

    /** Downloads a remote file to a local file. */
    public CompletableFuture<Transfer> downloadAsync(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions,
            CancellationToken cancellationToken) {
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
                defaultToken(cancellationToken));
    }

    /** Downloads data to a stream created by a factory. */
    public CompletableFuture<Transfer> downloadAsync(
            String requestedUsername, String remoteFilename, DownloadStreamFactory outputStreamFactory) {
        return downloadAsync(
                requestedUsername, remoteFilename, outputStreamFactory, null, 0, null, null, CancellationToken.none());
    }

    /** Downloads stream data with an expected size. */
    public CompletableFuture<Transfer> downloadAsync(
            String requestedUsername, String remoteFilename, DownloadStreamFactory outputStreamFactory, Long size) {
        return downloadAsync(
                requestedUsername, remoteFilename, outputStreamFactory, size, 0, null, null, CancellationToken.none());
    }

    /** Downloads stream data with cancellation. */
    public CompletableFuture<Transfer> downloadAsync(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            CancellationToken cancellationToken) {
        return downloadAsync(
                requestedUsername, remoteFilename, outputStreamFactory, null, 0, null, null, cancellationToken);
    }

    /** Downloads stream data from a resume offset. */
    public CompletableFuture<Transfer> downloadAsync(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset) {
        return downloadAsync(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                null,
                null,
                CancellationToken.none());
    }

    /** Downloads stream data with a specific token. */
    public CompletableFuture<Transfer> downloadAsync(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token) {
        return downloadAsync(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                token,
                null,
                CancellationToken.none());
    }

    /** Downloads stream data using supplied transfer options. */
    public CompletableFuture<Transfer> downloadAsync(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions) {
        return downloadAsync(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                token,
                transferOptions,
                CancellationToken.none());
    }

    /** Downloads data to a stream created by a factory. */
    public CompletableFuture<Transfer> downloadAsync(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions,
            CancellationToken cancellationToken) {
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
                defaultToken(cancellationToken));
    }

    /** Enqueues a local-file download. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String requestedUsername, String remoteFilename, String localFilename) {
        return enqueueDownloadAsync(
                requestedUsername, remoteFilename, localFilename, null, 0, null, null, CancellationToken.none());
    }

    /** Enqueues a local-file download with an expected size. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String requestedUsername, String remoteFilename, String localFilename, Long size) {
        return enqueueDownloadAsync(
                requestedUsername, remoteFilename, localFilename, size, 0, null, null, CancellationToken.none());
    }

    /** Enqueues a local-file download from a resume offset. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String requestedUsername, String remoteFilename, String localFilename, Long size, long startOffset) {
        return enqueueDownloadAsync(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                null,
                null,
                CancellationToken.none());
    }

    /** Enqueues a local-file download with a specific token. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token) {
        return enqueueDownloadAsync(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                token,
                null,
                CancellationToken.none());
    }

    /** Enqueues a local-file download using supplied options. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions) {
        return enqueueDownloadAsync(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                token,
                transferOptions,
                CancellationToken.none());
    }

    /** Enqueues a local-file download. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions,
            CancellationToken cancellationToken) {
        CompletableFuture<Boolean> enqueued = new CompletableFuture<>();
        TransferOptions options = (transferOptions == null ? new TransferOptions() : transferOptions)
                .withAdditionalStateChanged(change ->
                        completeDownloadEnqueue(enqueued, change.transfer().getState()));
        CompletableFuture<Transfer> download = downloadAsync(
                requestedUsername, remoteFilename, localFilename, size, startOffset, token, options, cancellationToken);
        return nestedDownloadWhenEnqueued(enqueued, download);
    }

    /** Enqueues a stream-factory download. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String requestedUsername, String remoteFilename, DownloadStreamFactory outputStreamFactory) {
        return enqueueDownloadAsync(
                requestedUsername, remoteFilename, outputStreamFactory, null, 0, null, null, CancellationToken.none());
    }

    /** Enqueues a stream-factory download with an expected size. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String requestedUsername, String remoteFilename, DownloadStreamFactory outputStreamFactory, Long size) {
        return enqueueDownloadAsync(
                requestedUsername, remoteFilename, outputStreamFactory, size, 0, null, null, CancellationToken.none());
    }

    /** Enqueues a stream-factory download from a resume offset. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset) {
        return enqueueDownloadAsync(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                null,
                null,
                CancellationToken.none());
    }

    /** Enqueues a stream-factory download with a specific token. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token) {
        return enqueueDownloadAsync(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                token,
                null,
                CancellationToken.none());
    }

    /** Enqueues a stream-factory download using supplied options. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions) {
        return enqueueDownloadAsync(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                token,
                transferOptions,
                CancellationToken.none());
    }

    /** Enqueues a stream-factory download. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions,
            CancellationToken cancellationToken) {
        CompletableFuture<Boolean> enqueued = new CompletableFuture<>();
        TransferOptions options = (transferOptions == null ? new TransferOptions() : transferOptions)
                .withAdditionalStateChanged(change ->
                        completeDownloadEnqueue(enqueued, change.transfer().getState()));
        CompletableFuture<Transfer> download = downloadAsync(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                token,
                options,
                cancellationToken);
        return nestedDownloadWhenEnqueued(enqueued, download);
    }

    /** Uploads a local file to a peer. */
    public CompletableFuture<Transfer> uploadAsync(
            String requestedUsername, String remoteFilename, String localFilename) {
        return uploadAsync(requestedUsername, remoteFilename, localFilename, null, null, CancellationToken.none());
    }

    /** Uploads a local file to a peer with a specific token. */
    public CompletableFuture<Transfer> uploadAsync(
            String requestedUsername, String remoteFilename, String localFilename, Integer token) {
        return uploadAsync(requestedUsername, remoteFilename, localFilename, token, null, CancellationToken.none());
    }

    /** Uploads a local file with cancellation. */
    public CompletableFuture<Transfer> uploadAsync(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            CancellationToken cancellationToken) {
        return uploadAsync(requestedUsername, remoteFilename, localFilename, null, null, cancellationToken);
    }

    /** Uploads a local file using the supplied options. */
    public CompletableFuture<Transfer> uploadAsync(
            String requestedUsername, String remoteFilename, String localFilename, TransferOptions transferOptions) {
        return uploadAsync(
                requestedUsername, remoteFilename, localFilename, null, transferOptions, CancellationToken.none());
    }

    /** Uploads a local file to a peer using the supplied options. */
    public CompletableFuture<Transfer> uploadAsync(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Integer token,
            TransferOptions transferOptions) {
        return uploadAsync(
                requestedUsername, remoteFilename, localFilename, token, transferOptions, CancellationToken.none());
    }

    /** Uploads a local file to a peer. */
    public CompletableFuture<Transfer> uploadAsync(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Integer token,
            TransferOptions transferOptions,
            CancellationToken cancellationToken) {
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
                defaultToken(cancellationToken));
    }

    /** Uploads data supplied by an asynchronous stream factory. */
    public CompletableFuture<Transfer> uploadAsync(
            String requestedUsername, String remoteFilename, long size, UploadStreamFactory inputStreamFactory) {
        return uploadAsync(
                requestedUsername, remoteFilename, size, inputStreamFactory, null, null, CancellationToken.none());
    }

    /** Uploads stream data with a specific transfer token. */
    public CompletableFuture<Transfer> uploadAsync(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token) {
        return uploadAsync(
                requestedUsername, remoteFilename, size, inputStreamFactory, token, null, CancellationToken.none());
    }

    /** Uploads stream data with cancellation. */
    public CompletableFuture<Transfer> uploadAsync(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            CancellationToken cancellationToken) {
        return uploadAsync(requestedUsername, remoteFilename, size, inputStreamFactory, null, null, cancellationToken);
    }

    /** Uploads stream data using the supplied options. */
    public CompletableFuture<Transfer> uploadAsync(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            TransferOptions transferOptions) {
        return uploadAsync(
                requestedUsername,
                remoteFilename,
                size,
                inputStreamFactory,
                null,
                transferOptions,
                CancellationToken.none());
    }

    /** Uploads stream data using the supplied transfer options. */
    public CompletableFuture<Transfer> uploadAsync(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token,
            TransferOptions transferOptions) {
        return uploadAsync(
                requestedUsername,
                remoteFilename,
                size,
                inputStreamFactory,
                token,
                transferOptions,
                CancellationToken.none());
    }

    /** Uploads data supplied by an asynchronous stream factory. */
    public CompletableFuture<Transfer> uploadAsync(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token,
            TransferOptions transferOptions,
            CancellationToken cancellationToken) {
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
                defaultToken(cancellationToken));
    }

    /** Enqueues a local-file upload and returns its nested completion future. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String requestedUsername, String remoteFilename, String localFilename) {
        return enqueueUploadAsync(
                requestedUsername, remoteFilename, localFilename, null, null, CancellationToken.none());
    }

    /** Enqueues a local-file upload with a specific token. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String requestedUsername, String remoteFilename, String localFilename, Integer token) {
        return enqueueUploadAsync(
                requestedUsername, remoteFilename, localFilename, token, null, CancellationToken.none());
    }

    /** Enqueues a local-file upload with cancellation. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            CancellationToken cancellationToken) {
        return enqueueUploadAsync(requestedUsername, remoteFilename, localFilename, null, null, cancellationToken);
    }

    /** Enqueues a local-file upload using supplied options. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Integer token,
            TransferOptions transferOptions) {
        return enqueueUploadAsync(
                requestedUsername, remoteFilename, localFilename, token, transferOptions, CancellationToken.none());
    }

    /** Enqueues a local-file upload. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Integer token,
            TransferOptions transferOptions,
            CancellationToken cancellationToken) {
        CompletableFuture<Boolean> enqueued = new CompletableFuture<>();
        TransferOptions options = (transferOptions == null ? new TransferOptions() : transferOptions)
                .withAdditionalStateChanged(change -> {
                    if (change.transfer().getState().equals(TransferStates.QUEUED.or(TransferStates.LOCALLY))) {
                        enqueued.complete(true);
                    }
                });
        CompletableFuture<Transfer> upload =
                uploadAsync(requestedUsername, remoteFilename, localFilename, token, options, cancellationToken);
        return enqueued.thenApply(ignored -> upload);
    }

    /** Enqueues a stream-factory upload. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String requestedUsername, String remoteFilename, long size, UploadStreamFactory inputStreamFactory) {
        return enqueueUploadAsync(
                requestedUsername, remoteFilename, size, inputStreamFactory, null, null, CancellationToken.none());
    }

    /** Enqueues a stream-factory upload with a specific token. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token) {
        return enqueueUploadAsync(
                requestedUsername, remoteFilename, size, inputStreamFactory, token, null, CancellationToken.none());
    }

    /** Enqueues a stream-factory upload with cancellation. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            CancellationToken cancellationToken) {
        return enqueueUploadAsync(
                requestedUsername, remoteFilename, size, inputStreamFactory, null, null, cancellationToken);
    }

    /** Enqueues a stream-factory upload using supplied options. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token,
            TransferOptions transferOptions) {
        return enqueueUploadAsync(
                requestedUsername,
                remoteFilename,
                size,
                inputStreamFactory,
                token,
                transferOptions,
                CancellationToken.none());
    }

    /** Enqueues a stream-factory upload. */
    public CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token,
            TransferOptions transferOptions,
            CancellationToken cancellationToken) {
        CompletableFuture<Boolean> enqueued = new CompletableFuture<>();
        TransferOptions options = (transferOptions == null ? new TransferOptions() : transferOptions)
                .withAdditionalStateChanged(change -> {
                    if (change.transfer().getState().equals(TransferStates.QUEUED.or(TransferStates.LOCALLY))) {
                        enqueued.complete(true);
                    }
                });
        CompletableFuture<Transfer> upload = uploadAsync(
                requestedUsername, remoteFilename, size, inputStreamFactory, token, options, cancellationToken);
        return enqueued.thenApply(ignored -> upload);
    }

    public CompletableFuture<Void> addPrivateRoomModeratorAsync(String roomName, String requestedUsername) {
        return addPrivateRoomModeratorAsync(roomName, requestedUsername, CancellationToken.none());
    }

    public CompletableFuture<Void> addPrivateRoomModeratorAsync(
            String roomName, String requestedUsername, CancellationToken cancellationToken) {
        requireText(roomName, "roomName");
        requireText(requestedUsername, "username");
        requireLoggedIn("add moderators to private rooms");
        return executeCorrelatedServerCommand(
                new PrivateRoomAddOperator(roomName, requestedUsername),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_ADD_OPERATOR, roomName, requestedUsername),
                cancellationToken,
                "Failed to add user " + requestedUsername + " as moderator of private room " + roomName + ": ");
    }

    public CompletableFuture<Void> removePrivateRoomMemberAsync(String roomName, String requestedUsername) {
        return removePrivateRoomMemberAsync(roomName, requestedUsername, CancellationToken.none());
    }

    public CompletableFuture<Void> removePrivateRoomMemberAsync(
            String roomName, String requestedUsername, CancellationToken cancellationToken) {
        requireText(roomName, "roomName");
        requireText(requestedUsername, "username");
        requireLoggedIn("remove users from private rooms");
        return executeCorrelatedServerCommand(
                new PrivateRoomRemoveUser(roomName, requestedUsername),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVE_USER, roomName, requestedUsername),
                cancellationToken,
                "Failed to remove user " + requestedUsername + " as member of private room " + roomName + ": ");
    }

    public CompletableFuture<Void> removePrivateRoomModeratorAsync(String roomName, String requestedUsername) {
        return removePrivateRoomModeratorAsync(roomName, requestedUsername, CancellationToken.none());
    }

    public CompletableFuture<Void> removePrivateRoomModeratorAsync(
            String roomName, String requestedUsername, CancellationToken cancellationToken) {
        requireText(roomName, "roomName");
        requireText(requestedUsername, "username");
        requireLoggedIn("remove moderators from private rooms");
        return executeCorrelatedServerCommand(
                new PrivateRoomRemoveOperator(roomName, requestedUsername),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVE_OPERATOR, roomName, requestedUsername),
                cancellationToken,
                "Failed to remove user " + requestedUsername + " as moderator of private room " + roomName + ": ");
    }

    public CompletableFuture<Void> dropPrivateRoomMembershipAsync(String roomName) {
        return dropPrivateRoomMembershipAsync(roomName, CancellationToken.none());
    }

    public CompletableFuture<Void> dropPrivateRoomMembershipAsync(
            String roomName, CancellationToken cancellationToken) {
        requireText(roomName, "roomName");
        requireLoggedIn("drop private room membership");
        return executeCorrelatedServerCommand(
                new PrivateRoomDropMembershipCommand(roomName),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVED, roomName),
                cancellationToken,
                "Failed to drop membership of private room " + roomName + ": ");
    }

    public CompletableFuture<Void> dropPrivateRoomOwnershipAsync(String roomName) {
        return dropPrivateRoomOwnershipAsync(roomName, CancellationToken.none());
    }

    public CompletableFuture<Void> dropPrivateRoomOwnershipAsync(String roomName, CancellationToken cancellationToken) {
        requireText(roomName, "roomName");
        requireLoggedIn("drop private room ownership");
        return executeCorrelatedServerCommand(
                new PrivateRoomDropOwnershipCommand(roomName),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVED, roomName),
                cancellationToken,
                "Failed to drop ownership of private room " + roomName + ": ");
    }

    public CompletableFuture<Void> sendRoomMessageAsync(String roomName, String message) {
        return sendRoomMessageAsync(roomName, message, CancellationToken.none());
    }

    public CompletableFuture<Void> sendRoomMessageAsync(
            String roomName, String message, CancellationToken cancellationToken) {
        requireText(roomName, "roomName");
        requireNonEmpty(message, "message");
        requireLoggedIn("send a chat room message");
        return writeServerAsync(
                new RoomMessageCommand(roomName, message),
                cancellationToken,
                "Failed to send message to room " + roomName + ": ");
    }

    public CompletableFuture<Void> sendUploadSpeedAsync(int speed) {
        return sendUploadSpeedAsync(speed, CancellationToken.none());
    }

    public CompletableFuture<Void> sendUploadSpeedAsync(int speed, CancellationToken cancellationToken) {
        requireLoggedIn("set upload speed");
        if (speed <= 0) {
            throw new IllegalArgumentException("The upload speed must be greater than zero");
        }
        return writeServerAsync(new SendUploadSpeedCommand(speed), cancellationToken, "Failed to set upload speed: ");
    }

    public CompletableFuture<Void> setRoomTickerAsync(String roomName, String message) {
        return setRoomTickerAsync(roomName, message, CancellationToken.none());
    }

    public CompletableFuture<Void> setRoomTickerAsync(
            String roomName, String message, CancellationToken cancellationToken) {
        requireText(roomName, "roomName");
        requireNonEmpty(message, "message");
        requireLoggedIn("set chat room tickers");
        return writeServerAsync(
                new SetRoomTickerCommand(roomName, message),
                cancellationToken,
                "Failed to set chat room ticker in room " + roomName + ": ");
    }

    public CompletableFuture<Void> setSharedCountsAsync(int directories, int files) {
        return setSharedCountsAsync(directories, files, CancellationToken.none());
    }

    public CompletableFuture<Void> setSharedCountsAsync(
            int directories, int files, CancellationToken cancellationToken) {
        if (directories < 0) {
            throw new IllegalArgumentException("The directory count must be equal to or greater than zero");
        }
        if (files < 0) {
            throw new IllegalArgumentException("The file count must be equal to or greater than zero");
        }
        requireLoggedIn("set shared counts");
        return writeServerAsync(
                new SetSharedCountsCommand(directories, files),
                cancellationToken,
                "Failed to set shared counts to " + directories + " directories and " + files + " files: ");
    }

    public CompletableFuture<Void> setStatusAsync(UserPresence status) {
        return setStatusAsync(status, CancellationToken.none());
    }

    public CompletableFuture<Void> setStatusAsync(UserPresence status, CancellationToken cancellationToken) {
        requireLoggedIn("set online status");
        return writeServerAsync(
                new SetOnlineStatusCommand(status), cancellationToken, "Failed to set user status to " + status + ": ");
    }

    public CompletableFuture<Void> startPublicChatAsync() {
        return startPublicChatAsync(CancellationToken.none());
    }

    public CompletableFuture<Void> startPublicChatAsync(CancellationToken cancellationToken) {
        requireLoggedIn("start public chat");
        return writeServerAsync(new StartPublicChatCommand(), cancellationToken, "Failed to start public chat: ");
    }

    public CompletableFuture<Void> stopPublicChatAsync() {
        return stopPublicChatAsync(CancellationToken.none());
    }

    public CompletableFuture<Void> stopPublicChatAsync(CancellationToken cancellationToken) {
        requireLoggedIn("stop public chat");
        return writeServerAsync(new StopPublicChatCommand(), cancellationToken, "Failed to stop public chat: ");
    }

    public CompletableFuture<Void> unwatchUserAsync(String requestedUsername) {
        return unwatchUserAsync(requestedUsername, CancellationToken.none());
    }

    public CompletableFuture<Void> unwatchUserAsync(String requestedUsername, CancellationToken cancellationToken) {
        requireText(requestedUsername, "username");
        requireLoggedIn("add users");
        return writeServerAsync(
                new UnwatchUserCommand(requestedUsername),
                cancellationToken,
                "Failed to unwatch user " + requestedUsername + ": ");
    }

    public CompletableFuture<Void> acknowledgePrivateMessageAsync(int privateMessageId) {
        return acknowledgePrivateMessageAsync(privateMessageId, CancellationToken.none());
    }

    @Override
    public CompletableFuture<Void> acknowledgePrivateMessageAsync(
            int privateMessageId, CancellationToken cancellationToken) {
        if (privateMessageId < 0) {
            throw new IllegalArgumentException("The private message ID must be greater than zero");
        }
        requireLoggedIn("acknowledge private messages");
        CompletableFuture<Void> write = writeServerAsync(
                new AcknowledgePrivateMessageCommand(privateMessageId),
                cancellationToken,
                "Failed to acknowledge private message with ID " + privateMessageId + ": ");
        return write.thenRun(() -> diagnostic.debug("Acknowledged private message ID " + privateMessageId));
    }

    public CompletableFuture<Void> acknowledgePrivilegeNotificationAsync(int privilegeNotificationId) {
        return acknowledgePrivilegeNotificationAsync(privilegeNotificationId, CancellationToken.none());
    }

    @Override
    public CompletableFuture<Void> acknowledgePrivilegeNotificationAsync(
            int privilegeNotificationId, CancellationToken cancellationToken) {
        if (privilegeNotificationId < 0) {
            throw new IllegalArgumentException("The privilege notification ID must be greater than zero");
        }
        requireLoggedIn("acknowledge privilege notifications");
        return writeServerAsync(
                new AcknowledgePrivilegeNotificationCommand(privilegeNotificationId),
                cancellationToken,
                "Failed to acknowledge privilege notification with ID " + privilegeNotificationId + ": ");
    }

    public CompletableFuture<InetSocketAddress> getUserEndPointAsync(String requestedUsername) {
        return getUserEndPointAsync(requestedUsername, CancellationToken.none());
    }

    @Override
    public CompletableFuture<InetSocketAddress> getUserEndPointAsync(
            String requestedUsername, CancellationToken cancellationToken) {
        requireText(requestedUsername, "username");
        requireLoggedIn("fetch user endpoint");
        CancellationToken token = defaultToken(cancellationToken);
        IUserEndPointCache cache = options.getUserEndPointCache();
        if (cache != null) {
            CacheLookupResult<InetSocketAddress> cached = tryCacheGet(cache, requestedUsername);
            if (cached.found()) {
                diagnostic.debug("EndPoint cache HIT for " + requestedUsername + ": " + cached.value());
                return CompletableFuture.completedFuture(cached.value());
            }
        }
        CompletableFuture<InetSocketAddress> request = endpointRequests.computeIfAbsent(
                requestedUsername, ignored -> retrieveUserEndPoint(requestedUsername, token, cache));
        request.whenComplete((result, failure) -> endpointRequests.remove(requestedUsername, request));
        return request;
    }

    /**
     * Searches the network and collects accepted responses.
     *
     * @param query the search query
     * @return the completed search and collected responses
     */
    public CompletableFuture<SearchResult> searchAsync(SearchQuery query) {
        return searchAsync(query, null, null, null, CancellationToken.none());
    }

    /**
     * Searches the network and collects accepted responses.
     *
     * @param query the search query
     * @param cancellationToken the cancellation token
     * @return the completed search and collected responses
     */
    public CompletableFuture<SearchResult> searchAsync(SearchQuery query, CancellationToken cancellationToken) {
        return searchAsync(query, null, null, null, cancellationToken);
    }

    /**
     * Searches the selected scope and collects accepted responses.
     *
     * @param query the search query
     * @param scope the search scope
     * @return the completed search and collected responses
     */
    public CompletableFuture<SearchResult> searchAsync(SearchQuery query, SearchScope scope) {
        return searchAsync(query, scope, null, null, CancellationToken.none());
    }

    /**
     * Searches the selected scope with a specific token.
     *
     * @param query the search query
     * @param scope the search scope
     * @param token the unique token
     * @return the completed search and collected responses
     */
    public CompletableFuture<SearchResult> searchAsync(SearchQuery query, SearchScope scope, Integer token) {
        return searchAsync(query, scope, token, null, CancellationToken.none());
    }

    /**
     * Searches the selected scope using the supplied options.
     *
     * @param query the search query
     * @param scope the search scope
     * @param token the unique token
     * @param searchOptions the search options
     * @return the completed search and collected responses
     */
    public CompletableFuture<SearchResult> searchAsync(
            SearchQuery query, SearchScope scope, Integer token, SearchOptions searchOptions) {
        return searchAsync(query, scope, token, searchOptions, CancellationToken.none());
    }

    /**
     * Searches the selected scope and collects accepted responses.
     *
     * @param query the search query
     * @param scope the search scope, or {@code null} for the network
     * @param token the unique token, or {@code null} to generate one
     * @param searchOptions the search options, or {@code null} for defaults
     * @param cancellationToken the cancellation token
     * @return the completed search and collected responses
     */
    public CompletableFuture<SearchResult> searchAsync(
            SearchQuery query,
            SearchScope scope,
            Integer token,
            SearchOptions searchOptions,
            CancellationToken cancellationToken) {
        SearchInvocation invocation = validateSearch(query, scope, token, searchOptions);
        List<SearchResponse> responses = Collections.synchronizedList(new ArrayList<>());
        return searchToCallbackAsync(invocation, responses::add, defaultToken(cancellationToken))
                .thenApply(search -> {
                    synchronized (responses) {
                        return new SearchResult(search, responses);
                    }
                });
    }

    /**
     * Searches the network and invokes a handler for each accepted response.
     *
     * @param query the search query
     * @param responseHandler the response handler
     * @return the completed search
     */
    public CompletableFuture<Search> searchAsync(SearchQuery query, Consumer<SearchResponse> responseHandler) {
        return searchAsync(query, responseHandler, null, null, null, CancellationToken.none());
    }

    /**
     * Searches the network and invokes a handler for each accepted response.
     *
     * @param query the search query
     * @param responseHandler the response handler
     * @param cancellationToken the cancellation token
     * @return the completed search
     */
    public CompletableFuture<Search> searchAsync(
            SearchQuery query, Consumer<SearchResponse> responseHandler, CancellationToken cancellationToken) {
        return searchAsync(query, responseHandler, null, null, null, cancellationToken);
    }

    /**
     * Searches the selected scope and invokes a response handler.
     *
     * @param query the search query
     * @param responseHandler the response handler
     * @param scope the search scope
     * @return the completed search
     */
    public CompletableFuture<Search> searchAsync(
            SearchQuery query, Consumer<SearchResponse> responseHandler, SearchScope scope) {
        return searchAsync(query, responseHandler, scope, null, null, CancellationToken.none());
    }

    /**
     * Searches the selected scope with a specific token.
     *
     * @param query the search query
     * @param responseHandler the response handler
     * @param scope the search scope
     * @param token the unique token
     * @return the completed search
     */
    public CompletableFuture<Search> searchAsync(
            SearchQuery query, Consumer<SearchResponse> responseHandler, SearchScope scope, Integer token) {
        return searchAsync(query, responseHandler, scope, token, null, CancellationToken.none());
    }

    /**
     * Searches the selected scope using the supplied options.
     *
     * @param query the search query
     * @param responseHandler the response handler
     * @param scope the search scope
     * @param token the unique token
     * @param searchOptions the search options
     * @return the completed search
     */
    public CompletableFuture<Search> searchAsync(
            SearchQuery query,
            Consumer<SearchResponse> responseHandler,
            SearchScope scope,
            Integer token,
            SearchOptions searchOptions) {
        return searchAsync(query, responseHandler, scope, token, searchOptions, CancellationToken.none());
    }

    /**
     * Searches the selected scope and invokes a handler for each accepted
     * response.
     *
     * @param query the search query
     * @param responseHandler the response handler
     * @param scope the search scope, or {@code null} for the network
     * @param token the unique token, or {@code null} to generate one
     * @param searchOptions the search options, or {@code null} for defaults
     * @param cancellationToken the cancellation token
     * @return the completed search
     */
    public CompletableFuture<Search> searchAsync(
            SearchQuery query,
            Consumer<SearchResponse> responseHandler,
            SearchScope scope,
            Integer token,
            SearchOptions searchOptions,
            CancellationToken cancellationToken) {
        SearchQuery validatedQuery = validateSearchQuery(query);
        Objects.requireNonNull(responseHandler, "responseHandler");
        SearchInvocation invocation = validateSearch(validatedQuery, scope, token, searchOptions);
        return searchToCallbackAsync(invocation, responseHandler, defaultToken(cancellationToken));
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
        if (state.equals(SoulseekClientStates.DISCONNECTED) || state.equals(SoulseekClientStates.DISCONNECTING)) {
            return;
        }
        changeState(SoulseekClientStates.DISCONNECTING, message, exception);
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
        changeState(SoulseekClientStates.DISCONNECTED, reason, exception);
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
        cleanupScheduler.shutdownNow();
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
    public final ISearchResponder getSearchResponder() {
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

    final Semaphore getUploadSemaphoreSyncRootForTest() {
        return uploadSemaphoreSyncRoot;
    }

    void setStateForTest(SoulseekClientStates value) {
        state = value;
    }

    void setServerConnectionForTest(MessageConnection value) {
        serverConnection = value;
    }

    void setListenerForTest(Listener value) {
        listener = value;
    }

    void setIpEndPointForTest(InetSocketAddress value) {
        ipEndPoint = value;
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

    void changeState(SoulseekClientStates newState, String message, Exception exception) {
        SoulseekClientStates previousState = state;
        state = newState;
        diagnostic.debug("Client state changed from " + previousState + " to "
                + newState
                + (message == null ? "" : "; message: " + message));
        raise(Event.STATE_CHANGED, new SoulseekClientStateChangedEventArgs(previousState, state, message, exception));
        if (state.equals(SoulseekClientStates.CONNECTED)) {
            raise(Event.CONNECTED, null);
        } else if (state.equals(SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGED_IN))) {
            raise(Event.LOGGED_IN, null);
        } else if (state.equals(SoulseekClientStates.DISCONNECTED)) {
            raise(Event.DISCONNECTED, new SoulseekClientDisconnectedEventArgs(message, exception));
        }
    }

    private void bindEvents() {
        listenerHandler.addDiagnosticGeneratedListener(
                (sender, eventArgs) -> raiseFrom(sender, Event.DIAGNOSTIC_GENERATED, eventArgs));
        searchResponder.addDiagnosticGeneratedListener(
                (sender, eventArgs) -> raiseFrom(sender, Event.DIAGNOSTIC_GENERATED, eventArgs));
        searchResponder.addRequestReceivedListener(
                (sender, eventArgs) -> raise(Event.SEARCH_REQUEST_RECEIVED, eventArgs));
        searchResponder.addResponseDeliveredListener(
                (sender, eventArgs) -> raise(Event.SEARCH_RESPONSE_DELIVERED, eventArgs));
        searchResponder.addResponseDeliveryFailedListener(
                (sender, eventArgs) -> raise(Event.SEARCH_RESPONSE_DELIVERY_FAILED, eventArgs));

        peerMessageHandler.addDiagnosticGeneratedListener(
                (sender, eventArgs) -> raiseFrom(sender, Event.DIAGNOSTIC_GENERATED, eventArgs));
        peerMessageHandler.addDownloadDeniedListener((sender, eventArgs) -> downloadDenied(eventArgs));
        peerMessageHandler.addDownloadFailedListener((sender, eventArgs) -> downloadFailed(eventArgs));
        distributedMessageHandler.addDiagnosticGeneratedListener(
                (sender, eventArgs) -> raiseFrom(sender, Event.DIAGNOSTIC_GENERATED, eventArgs));
        peerConnectionManager.addDiagnosticGeneratedListener(
                (sender, eventArgs) -> raiseFrom(sender, Event.DIAGNOSTIC_GENERATED, eventArgs));
        distributedConnectionManager.addDiagnosticGeneratedListener(
                (sender, eventArgs) -> raiseFrom(sender, Event.DIAGNOSTIC_GENERATED, eventArgs));
        distributedConnectionManager.addPromotedToBranchRootListener(
                (sender, eventArgs) -> raise(Event.PROMOTED_TO_DISTRIBUTED_BRANCH_ROOT, null));
        distributedConnectionManager.addDemotedFromBranchRootListener(
                (sender, eventArgs) -> raise(Event.DEMOTED_FROM_DISTRIBUTED_BRANCH_ROOT, null));
        distributedConnectionManager.addParentAdoptedListener(
                (sender, eventArgs) -> raise(Event.DISTRIBUTED_PARENT_ADOPTED, eventArgs));
        distributedConnectionManager.addParentDisconnectedListener(
                (sender, eventArgs) -> raise(Event.DISTRIBUTED_PARENT_DISCONNECTED, eventArgs));
        distributedConnectionManager.addChildAddedListener(
                (sender, eventArgs) -> raise(Event.DISTRIBUTED_CHILD_ADDED, eventArgs));
        distributedConnectionManager.addChildDisconnectedListener(
                (sender, eventArgs) -> raise(Event.DISTRIBUTED_CHILD_DISCONNECTED, eventArgs));
        distributedConnectionManager.addStateChangedListener(
                (sender, eventArgs) -> raise(Event.DISTRIBUTED_NETWORK_STATE_CHANGED, eventArgs));

        serverMessageHandler.addDiagnosticGeneratedListener(
                (sender, eventArgs) -> raiseFrom(sender, Event.DIAGNOSTIC_GENERATED, eventArgs));
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
        serverMessageHandler.<ServerInfo>addListener(ServerMessageEvent.SERVER_INFO_RECEIVED, (sender, eventArgs) -> {
            serverInfo = serverInfo.with(
                    eventArgs.getParentMinSpeed(),
                    eventArgs.getParentSpeedRatio(),
                    eventArgs.getWishlistInterval(),
                    eventArgs.isSupporter());
            raise(Event.SERVER_INFO_RECEIVED, serverInfo);
        });
        serverMessageHandler.<Void>addListener(ServerMessageEvent.KICKED_FROM_SERVER, (sender, eventArgs) -> {
            diagnostic.info("Kicked from server.");
            raise(Event.KICKED_FROM_SERVER, null);
            disconnect("Kicked from server", new KickedFromServerException());
        });
    }

    private <T> void forwardServer(ServerMessageEvent source, Event target) {
        serverMessageHandler.<T>addListener(source, (sender, eventArgs) -> raise(target, eventArgs));
    }

    private void downloadDenied(DownloadDeniedEventArgs eventArgs) {
        try {
            List<TransferInternal> matching = downloads.values().stream()
                    .filter(download -> Objects.equals(download.getUsername(), eventArgs.getUsername())
                            && Objects.equals(download.getFilename(), eventArgs.getFilename()))
                    .toList();
            for (TransferInternal download : matching) {
                download.getRemoteTaskCompletionSource()
                        .completeExceptionally(new TransferRejectedException(eventArgs.getMessage()));
                diagnostic.debug("Download of " + download.getFilename() + " from "
                        + download.getUsername()
                        + " rejected by remote client (token: "
                        + download.getToken() + ")");
            }
        } catch (Throwable failure) {
            diagnostic.warning("Failed to mark download(s) rejected: " + failureMessage(failure), failure);
        } finally {
            raise(Event.DOWNLOAD_DENIED, eventArgs);
        }
    }

    private void downloadFailed(DownloadFailedEventArgs eventArgs) {
        try {
            List<TransferInternal> matching = downloads.values().stream()
                    .filter(download -> Objects.equals(download.getUsername(), eventArgs.getUsername())
                            && Objects.equals(download.getFilename(), eventArgs.getFilename()))
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
            raise(Event.DOWNLOAD_FAILED, eventArgs);
        }
    }

    private <T> void add(Event event, SoulseekClientEventListener<T> listener) {
        listeners.get(event).add(Objects.requireNonNull(listener, "listener"));
    }

    private <T> void remove(Event event, SoulseekClientEventListener<T> listener) {
        listeners.get(event).remove(listener);
    }

    private <T> void raise(Event event, T eventArgs) {
        raiseFrom(this, event, eventArgs);
    }

    @SuppressWarnings("unchecked")
    private <T> void raiseFrom(Object sender, Event event, T eventArgs) {
        for (SoulseekClientEventListener<?> listener : listeners.get(event)) {
            ((SoulseekClientEventListener<T>) listener).handle(sender, eventArgs);
        }
    }

    private static String failureMessage(Throwable failure) {
        return failure.getMessage() == null ? "" : failure.getMessage();
    }

    private void requireLoggedIn(String operation) {
        if (!state.hasFlag(SoulseekClientStates.CONNECTED) || !state.hasFlag(SoulseekClientStates.LOGGED_IN)) {
            throw new IllegalStateException("The server connection must be connected and logged in to " + operation
                    + " (currently: " + state + ")");
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be null, empty, or whitespace");
        }
    }

    private static void requireNonEmpty(String value, String name) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be null or empty");
        }
    }

    private void updateBrowseProgress(
            String requestedUsername,
            BrowseOptions operationOptions,
            long bytesTransferred,
            long size,
            AtomicBoolean completionEventFired) {
        BrowseProgressUpdatedEventArgs eventArgs =
                new BrowseProgressUpdatedEventArgs(requestedUsername, bytesTransferred, size);
        if (Double.compare(eventArgs.getPercentComplete(), 100.0) == 0) {
            completionEventFired.set(true);
        }
        if (operationOptions.getProgressUpdated() != null) {
            operationOptions
                    .getProgressUpdated()
                    .onProgressUpdated(new BrowseProgress(
                            eventArgs.getUsername(),
                            eventArgs.getBytesTransferred(),
                            eventArgs.getBytesRemaining(),
                            eventArgs.getPercentComplete(),
                            eventArgs.getSize()));
        }
        raise(Event.BROWSE_PROGRESS_UPDATED, eventArgs);
    }

    private CompletableFuture<Void> connectInternalAsync(
            String requestedAddress,
            InetSocketAddress requestedEndPoint,
            String requestedUsername,
            String password,
            CancellationToken cancellationToken) {
        CompletableFuture<Void> serialized = acquirePermit(stateSemaphore, cancellationToken)
                .thenCompose(ignored -> {
                    CompletableFuture<Void> attempt;
                    if (state.hasFlag(SoulseekClientStates.CONNECTED)
                            && state.hasFlag(SoulseekClientStates.LOGGED_IN)) {
                        attempt = CompletableFuture.completedFuture(null);
                    } else {
                        attempt = performConnectAsync(
                                requestedAddress, requestedEndPoint, requestedUsername, password, cancellationToken);
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
            InetSocketAddress requestedEndPoint,
            String requestedUsername,
            String password,
            CancellationToken cancellationToken) {
        try {
            changeState(SoulseekClientStates.CONNECTING, "Connecting", null);

            if (options.isEnableListener()) {
                listener = clientListenerFactory.create(
                        options.getListenIPAddress(), options.getListenPort(), options.getIncomingConnectionOptions());
                listener.addAcceptedListener(listenerHandler::handleConnection);
                listener.start();
            }

            serverConnection = connectionFactory.getServerConnection(
                    requestedEndPoint,
                    (sender, eventArgs) ->
                            changeState(SoulseekClientStates.CONNECTED, "Connected to " + ipEndPoint, null),
                    (sender, eventArgs) -> disconnect(eventArgs.getMessage(), eventArgs.getException()),
                    serverMessageHandler::handleMessageRead,
                    serverMessageHandler::handleMessageWritten,
                    options.getServerConnectionOptions());

            CompletableFuture<Void> connectOperation;
            try {
                connectOperation = serverConnection.connectAsync(cancellationToken);
            } catch (Throwable failure) {
                connectOperation = CompletableFuture.failedFuture(failure);
            }
            return connectOperation.thenCompose(ignored -> {
                address = requestedAddress;
                ipEndPoint = requestedEndPoint;
                changeState(SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGING_IN), "Logging in", null);
                return loginAsync(requestedUsername, password, cancellationToken);
            });
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<Void> loginAsync(
            String requestedUsername, String password, CancellationToken cancellationToken) {
        CompletableFuture<LoginResponse> loginWait;
        try {
            loginWait = waiter.waitAsync(
                    new WaitKey(MessageCode.Server.LOGIN), LoginResponse.class, null, cancellationToken);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }

        ByteArrayOutputStream loginMessages = new ByteArrayOutputStream();
        loginMessages.writeBytes(new LoginRequest(minorVersion, requestedUsername, password).toByteArray());
        loginMessages.writeBytes(new SetListenPortCommand(options.getListenPort()).toByteArray());

        return invokeServerByteWrite(loginMessages.toByteArray(), cancellationToken)
                .thenCompose(ignored -> loginWait)
                .thenCompose(response -> {
                    if (!response.isSucceeded()) {
                        return CompletableFuture.failedFuture(new LoginRejectedException(
                                "The server rejected login attempt: " + response.getMessage()));
                    }
                    serverInfo = serverInfo.with(null, null, null, response.isSupporter());
                    raise(Event.SERVER_INFO_RECEIVED, serverInfo);
                    username = requestedUsername;
                    changeState(SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGED_IN), "Logged in", null);
                    return sendConfigurationMessagesAsync(cancellationToken);
                });
    }

    private CompletableFuture<Void> sendConfigurationMessagesAsync(CancellationToken cancellationToken) {
        return invokeServerWrite(new SetListenPortCommand(options.getListenPort()), cancellationToken)
                .thenCompose(ignored -> invokeServerWrite(
                        new PrivateRoomToggle(options.isAcceptPrivateRoomInvitations()), cancellationToken))
                .thenCompose(ignored -> {
                    try {
                        return distributedConnectionManager.updateStatusAsync(cancellationToken);
                    } catch (Throwable failure) {
                        return CompletableFuture.failedFuture(failure);
                    }
                });
    }

    private CompletableFuture<Boolean> reconfigureOptionsInternalAsync(
            SoulseekClientOptionsPatch patch, CancellationToken cancellationToken) {
        CompletableFuture<Boolean> serialized = acquirePermit(stateSemaphore, cancellationToken)
                .thenCompose(ignored -> {
                    CompletableFuture<Boolean> operation;
                    try {
                        operation = performReconfigureOptionsAsync(patch, cancellationToken);
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
            SoulseekClientOptionsPatch patch, CancellationToken cancellationToken) {
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
        boolean listenAddressChanged = patch.getListenIPAddress() != null
                && !patch.getListenIPAddress().equals(options.getListenIPAddress());
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
                        options.getListenIPAddress(), options.getListenPort(), options.getIncomingConnectionOptions());
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
        return sendConfigurationMessagesAsync(cancellationToken).thenApply(ignored -> {
            if (requiresReconnect) {
                diagnostic.warning("Server reconnect required for options " + "to fully take effect");
            }
            return requiresReconnect;
        });
    }

    private boolean isConnectedAndLoggedIn() {
        return state.hasFlag(SoulseekClientStates.CONNECTED) && state.hasFlag(SoulseekClientStates.LOGGED_IN);
    }

    private static SoulseekClientOptionsPatch listenerPatch(SoulseekClientOptionsPatch patch) {
        return new SoulseekClientOptionsPatch(
                patch.getEnableListener(),
                patch.getListenIPAddress(),
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

    private SearchInvocation validateSearch(
            SearchQuery initialQuery, SearchScope initialScope, Integer initialToken, SearchOptions initialOptions) {
        SearchQuery query = validateSearchQuery(initialQuery);
        requireLoggedIn("perform a search");

        int token = initialToken == null ? tokenFactory.nextToken() : initialToken;
        if (searches.containsKey(token)) {
            throw new DuplicateTokenException("An active search with token " + token + " is already in progress");
        }

        SearchScope scope = initialScope == null ? SearchScope.getNetwork() : initialScope;
        SearchOptions searchOptions = initialOptions == null ? new SearchOptions() : initialOptions;
        if (searchOptions.isRemoveSingleCharacterSearchTerms()) {
            query = new SearchQuery(
                    query.getTerms().stream().filter(term -> term.length() > 1).toList(), query.getExclusions());
        }
        if (query.getTerms().isEmpty()) {
            throw new IllegalArgumentException(
                    "Search query must contain at least one non-exclusion " + "term with length greater than 1");
        }
        return new SearchInvocation(query, scope, token, searchOptions);
    }

    private static SearchQuery validateSearchQuery(SearchQuery initialQuery) {
        SearchQuery query = Objects.requireNonNull(initialQuery, "query");
        if (query.getSearchText() == null || query.getSearchText().trim().isEmpty()) {
            throw new IllegalArgumentException("Search text must not be null, empty, or whitespace");
        }
        if (query.getTerms().isEmpty()) {
            throw new IllegalArgumentException("Search query must contain at least one " + "non-exclusion term");
        }
        return query;
    }

    private CompletableFuture<Search> searchToCallbackAsync(
            SearchInvocation invocation,
            Consumer<SearchResponse> responseHandler,
            CancellationToken cancellationToken) {
        SearchInternal search =
                new SearchInternal(invocation.query(), invocation.scope(), invocation.token(), invocation.options());
        SearchStates[] previousState = {SearchStates.NONE};
        Consumer<SearchStates> updateState = newState -> {
            search.setState(newState);
            Search snapshot = search.toSearch();
            SearchStateChangedEventArgs eventArgs = new SearchStateChangedEventArgs(previousState[0], snapshot);
            previousState[0] = newState;
            if (invocation.options().getStateChanged() != null) {
                invocation
                        .options()
                        .getStateChanged()
                        .onStateChanged(new SearchStateChange(eventArgs.getPreviousState(), eventArgs.getSearch()));
            }
            raise(Event.SEARCH_STATE_CHANGED, eventArgs);
        };

        CompletableFuture<Search> operation;
        try {
            searches.putIfAbsent(search.getToken(), search);
            updateState.accept(SearchStates.REQUESTED);
            diagnostic.debug("Attempting to acquire search semaphore for search '"
                    + invocation.query().getSearchText() + "' ("
                    + searchSemaphore.availablePermits()
                    + " available)");
            updateState.accept(SearchStates.QUEUED);
            operation = acquireSearchPermit(cancellationToken).thenCompose(ignored -> {
                diagnostic.debug("Acquired search semaphore for search '"
                        + invocation.query().getSearchText() + "'");
                CompletableFuture<Search> activeSearch;
                try {
                    byte[] message = buildSearchMessage(invocation.scope(), search);
                    search.setResponseReceived(response -> {
                        responseHandler.accept(response);
                        SearchResponseReceivedEventArgs eventArgs =
                                new SearchResponseReceivedEventArgs(response, search.toSearch());
                        if (invocation.options().getResponseReceived() != null) {
                            invocation
                                    .options()
                                    .getResponseReceived()
                                    .onResponseReceived(
                                            new SearchResponseReceived(eventArgs.getSearch(), eventArgs.getResponse()));
                        }
                        raise(Event.SEARCH_RESPONSE_RECEIVED, eventArgs);
                    });
                    activeSearch = invokeServerByteWrite(message, cancellationToken)
                            .thenRun(() -> updateState.accept(SearchStates.IN_PROGRESS))
                            .thenCompose(ignoredWrite -> search.waitForCompletion(cancellationToken))
                            .thenApply(ignoredCompletion -> {
                                updateState.accept(SearchStates.COMPLETED.or(search.getState()));
                                diagnostic.debug("Search for '"
                                        + invocation.query().getSearchText()
                                        + "' completed: "
                                        + search.getState());
                                return search.toSearch();
                            });
                } catch (Throwable failure) {
                    activeSearch = CompletableFuture.failedFuture(failure);
                }
                return activeSearch.whenComplete((result, failure) -> {
                    searchSemaphore.release();
                    diagnostic.debug("Released search semaphore for search '"
                            + invocation.query().getSearchText()
                            + "' ("
                            + searchSemaphore.availablePermits()
                            + " available)");
                });
            });
        } catch (Throwable failure) {
            operation = CompletableFuture.failedFuture(failure);
        }

        return operation
                .handle((result, failure) -> {
                    if (failure == null) {
                        return result;
                    }
                    Throwable cause = unwrap(failure);
                    if (cause instanceof CancellationException) {
                        search.complete(SearchStates.CANCELLED);
                        updateState.accept(SearchStates.COMPLETED.or(SearchStates.CANCELLED));
                        throw new CompletionException(cause);
                    }
                    search.complete(SearchStates.ERRORED);
                    updateState.accept(SearchStates.COMPLETED.or(SearchStates.ERRORED));
                    if (cause instanceof TimeoutException) {
                        throw new CompletionException(cause);
                    }
                    throw new CompletionException(new SoulseekClientException(
                            "Failed to search for "
                                    + invocation.query().getSearchText()
                                    + " (" + invocation.token() + "): "
                                    + failureMessage(cause),
                            cause));
                })
                .whenComplete((result, failure) -> {
                    searches.remove(search.getToken(), search);
                    search.close();
                });
    }

    private CompletableFuture<Void> acquireSearchPermit(CancellationToken cancellationToken) {
        return acquirePermit(searchSemaphore, cancellationToken);
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

    private static void completeDownloadEnqueue(CompletableFuture<Boolean> enqueued, TransferStates state) {
        if (state.equals(TransferStates.QUEUED.or(TransferStates.REMOTELY))) {
            enqueued.complete(true);
        } else if (state.hasFlag(TransferStates.COMPLETED) && !state.hasFlag(TransferStates.SUCCEEDED)) {
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
            CancellationToken cancellationToken) {
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
                new DownloadOperation(download, outputStreamFactory, operationOptions, cancellationToken, uniqueKey);
        return CompletableFuture.supplyAsync(operation::execute);
    }

    private final class DownloadOperation {
        private final TransferInternal download;
        private final DownloadStreamFactory outputStreamFactory;
        private final TransferOptions transferOptions;
        private final CancellationToken cancellationToken;
        private final String uniqueKey;
        private final AtomicBoolean globalPermit = new AtomicBoolean();
        private final CompletableFuture<Void> disconnected = new CompletableFuture<>();
        private final WaitKey transferStartRequestedWaitKey;
        private TransferStates lastState = TransferStates.NONE;
        private InetSocketAddress endpoint;
        private Connection connection;
        private OutputStream outputStream;
        private PositionTrackingOutputStream trackingStream;
        private ConnectionEventListener<ConnectionDataEventArgs> dataReadListener;
        private ConnectionEventListener<ConnectionDisconnectedEventArgs> disconnectedListener;

        private DownloadOperation(
                TransferInternal download,
                DownloadStreamFactory outputStreamFactory,
                TransferOptions transferOptions,
                CancellationToken cancellationToken,
                String uniqueKey) {
            this.download = download;
            this.outputStreamFactory = outputStreamFactory;
            this.transferOptions = transferOptions;
            this.cancellationToken = cancellationToken;
            this.uniqueKey = uniqueKey;
            transferStartRequestedWaitKey =
                    new WaitKey(MessageCode.Peer.TRANSFER_REQUEST, download.getUsername(), download.getFilename());
        }

        private Transfer execute() {
            try {
                updateState(TransferStates.QUEUED.or(TransferStates.LOCALLY));
                await(acquirePermit(globalDownloadSemaphore, cancellationToken));
                globalPermit.set(true);

                endpoint = await(getUserEndPointAsync(download.getUsername(), cancellationToken));
                MessageConnection peerConnection = await(peerConnectionManager.getOrAddMessageConnectionAsync(
                        download.getUsername(), endpoint, cancellationToken));

                CompletableFuture<TransferResponse> transferRequestAcknowledged = waiter.waitAsync(
                        new WaitKey(MessageCode.Peer.TRANSFER_RESPONSE, download.getUsername(), download.getToken()),
                        TransferResponse.class,
                        options.getPeerConnectionOptions().getInactivityTimeout(),
                        cancellationToken);
                CompletableFuture<TransferRequest> transferStartRequested = waiter.waitIndefinitelyAsync(
                        transferStartRequestedWaitKey, TransferRequest.class, cancellationToken);

                await(invokeMessageWrite(
                        peerConnection,
                        new TransferRequest(TransferDirection.DOWNLOAD, download.getToken(), download.getFilename()),
                        cancellationToken));
                updateState(TransferStates.REQUESTED);

                TransferResponse acknowledgement = await(transferRequestAcknowledged);
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
                updateState(TransferStates.COMPLETED.or(TransferStates.SUCCEEDED));
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
            updateState(TransferStates.QUEUED.or(TransferStates.REMOTELY));
            if (download.getSize() == null) {
                download.setSize(acknowledgement.getFileSize());
            }
            updateState(TransferStates.INITIALIZING);
            connection = await(peerConnectionManager.getTransferConnectionAsync(
                    download.getUsername(), endpoint, acknowledgement.getToken(), cancellationToken));
            download.setConnection(connection);
            return peerConnection;
        }

        private MessageConnection beginQueuedDownload(
                CompletableFuture<TransferRequest> transferStartRequested, MessageConnection peerConnection) {
            updateState(TransferStates.QUEUED.or(TransferStates.REMOTELY));
            TransferRequest request = await(transferStartRequested);
            validateRemoteSize(request.getFileSize());
            if (download.getSize() == null) {
                download.setSize(request.getFileSize());
            }
            download.setRemoteToken(request.getToken());
            updateState(TransferStates.INITIALIZING);

            MessageConnection refreshed = await(peerConnectionManager.getOrAddMessageConnectionAsync(
                    download.getUsername(), endpoint, cancellationToken));
            CompletableFuture<Connection> connectionTask = peerConnectionManager.awaitTransferConnectionAsync(
                    download.getUsername(), download.getFilename(), download.getRemoteToken(), cancellationToken);
            await(invokeMessageWrite(
                    refreshed,
                    new TransferResponse(
                            download.getRemoteToken(), download.getSize() == null ? 0 : download.getSize()),
                    cancellationToken));
            try {
                connection = await(connectionTask);
            } catch (Throwable failure) {
                Throwable cause = unwrap(failure);
                if (!(cause instanceof ConnectionException)) {
                    throw failure;
                }
                connection = await(peerConnectionManager.getTransferConnectionAsync(
                        download.getUsername(), endpoint, download.getRemoteToken(), cancellationToken));
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
                    (sender, eventArgs) -> updateProgress(download.getStartOffset() + eventArgs.getCurrentLength());
            disconnectedListener = (sender, eventArgs) -> {
                Throwable failure = eventArgs.getException();
                if (failure instanceof CancellationException || failure instanceof TimeoutException) {
                    disconnected.completeExceptionally(failure);
                } else {
                    disconnected.completeExceptionally(
                            new ConnectionException("Transfer failed: " + eventArgs.getMessage(), failure));
                }
            };
            connection.addDataReadListener(dataReadListener);
            connection.addDisconnectedListener(disconnectedListener);
        }

        private void positionOutputStream() {
            if (download.getStartOffset() <= 0 || !transferOptions.isSeekOutputStreamAutomatically()) {
                return;
            }
            try {
                seekOutputStream(outputStream, download.getStartOffset());
            } catch (IOException failure) {
                throw new TransferStreamException(
                        "Requested non-zero start offset but output " + "stream does not support seeking", failure);
            }
        }

        private void readTransfer() {
            try (CancellationTokenSource linkedSource = new CancellationTokenSource();
                    CancellationRegistration registration = cancellationToken.register(linkedSource::cancel)) {
                CancellationToken linkedToken = linkedSource.getToken();
                byte[] offset = ByteBuffer.allocate(8)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putLong(download.getStartOffset())
                        .array();
                await(connection.writeAsync(offset, linkedToken));
                updateState(TransferStates.IN_PROGRESS);
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
                linkedSource.cancel();
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
                updateState(TransferStates.COMPLETED.or(TransferStates.REJECTED));
                return;
            }
            if (failure instanceof TransferSizeMismatchException) {
                download.setException(failure);
                updateState(TransferStates.COMPLETED.or(TransferStates.ABORTED));
                return;
            }
            if (failure instanceof CancellationException) {
                disconnectTransfer("Transfer cancelled", failure);
                download.setException(failure);
                updateProgress(currentOutputPosition());
                updateState(TransferStates.COMPLETED.or(TransferStates.CANCELLED));
                return;
            }
            if (failure instanceof TimeoutException) {
                disconnectTransfer("Transfer timed out", failure);
                download.setException(failure);
                updateProgress(currentOutputPosition());
                updateState(TransferStates.COMPLETED.or(TransferStates.TIMED_OUT));
                return;
            }
            disconnectTransfer("Transfer error", failure);
            download.setException(failure);
            updateProgress(currentOutputPosition());
            updateState(TransferStates.COMPLETED.or(TransferStates.ERRORED));
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

        private void updateState(TransferStates state) {
            download.setState(state);
            Transfer transfer = download.toTransfer();
            TransferStateChangedEventArgs eventArgs = new TransferStateChangedEventArgs(lastState, transfer);
            TransferStates previous = lastState;
            lastState = state;
            if (transferOptions.getStateChanged() != null) {
                transferOptions.getStateChanged().onStateChanged(new TransferStateChange(previous, transfer));
            }
            raise(Event.TRANSFER_STATE_CHANGED, eventArgs);
        }

        private void updateProgress(long bytesDownloaded) {
            long previous = download.getBytesTransferred();
            download.updateProgress(bytesDownloaded);
            Transfer transfer = download.toTransfer();
            if (transferOptions.getProgressUpdated() != null) {
                transferOptions.getProgressUpdated().onProgressUpdated(new TransferProgressUpdate(previous, transfer));
            }
            raise(Event.TRANSFER_PROGRESS_UPDATED, new TransferProgressUpdatedEventArgs(previous, transfer));
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
            CancellationToken cancellationToken) {
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
                new UploadOperation(upload, inputStreamFactory, operationOptions, cancellationToken, uniqueKey);
        return CompletableFuture.supplyAsync(operation::execute);
    }

    private static String uploadUniqueKey(String requestedUsername, String remoteFilename) {
        return "Upload:" + requestedUsername + ":" + remoteFilename;
    }

    private final class UploadOperation {
        private final TransferInternal upload;
        private final UploadStreamFactory inputStreamFactory;
        private final TransferOptions transferOptions;
        private final CancellationToken cancellationToken;
        private final String uniqueKey;
        private final AtomicBoolean perUserPermit = new AtomicBoolean();
        private final AtomicBoolean slot = new AtomicBoolean();
        private final AtomicBoolean globalPermit = new AtomicBoolean();
        private final CompletableFuture<Void> disconnected = new CompletableFuture<>();
        private TransferStates lastState = TransferStates.NONE;
        private Semaphore perUserSemaphore;
        private InetSocketAddress endpoint;
        private Connection connection;
        private InputStream inputStream;
        private PositionTrackingInputStream trackingStream;
        private ConnectionEventListener<ConnectionDataEventArgs> dataWrittenListener;
        private ConnectionEventListener<ConnectionDisconnectedEventArgs> disconnectedListener;

        private UploadOperation(
                TransferInternal upload,
                UploadStreamFactory inputStreamFactory,
                TransferOptions transferOptions,
                CancellationToken cancellationToken,
                String uniqueKey) {
            this.upload = upload;
            this.inputStreamFactory = inputStreamFactory;
            this.transferOptions = transferOptions;
            this.cancellationToken = cancellationToken;
            this.uniqueKey = uniqueKey;
        }

        private Transfer execute() {
            try {
                await(acquirePermit(uploadSemaphoreSyncRoot, cancellationToken));
                CompletableFuture<Void> perUserWait;
                try {
                    perUserSemaphore = uploadSemaphores.computeIfAbsent(
                            upload.getUsername(),
                            ignored -> new Semaphore(options.getMaximumConcurrentUploadsPerUser()));
                    perUserWait = acquirePermit(perUserSemaphore, cancellationToken);
                } finally {
                    uploadSemaphoreSyncRoot.release();
                }

                updateState(TransferStates.QUEUED.or(TransferStates.LOCALLY));

                await(perUserWait);
                perUserPermit.set(true);
                diagnostic.debug("Upload semaphore for file "
                        + filenameOnly(upload.getFilename()) + " to "
                        + upload.getUsername() + " acquired");

                try {
                    await(transferOptions.getSlotAwaiter().awaitSlotAsync(upload.toTransfer(), cancellationToken));
                    slot.set(true);
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

                await(acquirePermit(globalUploadSemaphore, cancellationToken));
                globalPermit.set(true);

                endpoint = await(getUserEndPointAsync(upload.getUsername(), cancellationToken));
                MessageConnection messageConnection = await(peerConnectionManager.getOrAddMessageConnectionAsync(
                        upload.getUsername(), endpoint, cancellationToken));

                CompletableFuture<TransferResponse> transferRequestAcknowledged = waiter.waitAsync(
                        new WaitKey(MessageCode.Peer.TRANSFER_RESPONSE, upload.getUsername(), upload.getToken()),
                        TransferResponse.class,
                        options.getPeerConnectionOptions().getInactivityTimeout(),
                        cancellationToken);
                await(invokeMessageWrite(
                        messageConnection,
                        new TransferRequest(
                                TransferDirection.UPLOAD, upload.getToken(), upload.getFilename(), upload.getSize()),
                        cancellationToken));
                updateState(TransferStates.REQUESTED);

                TransferResponse acknowledgement = await(transferRequestAcknowledged);
                if (!acknowledgement.isAllowed()) {
                    throw new TransferRejectedException("Transfer rejected: " + acknowledgement.getMessage());
                }

                updateState(TransferStates.INITIALIZING);
                connection = await(peerConnectionManager.getTransferConnectionAsync(
                        upload.getUsername(), endpoint, upload.getToken(), cancellationToken));
                upload.setConnection(connection);
                bindConnectionEvents();

                readStartOffset();
                if (upload.getStartOffset() > upload.getSize()) {
                    throw new TransferException("Requested start offset of "
                            + upload.getStartOffset()
                            + " bytes exceeds file length of "
                            + upload.getSize() + " bytes");
                }

                inputStream = Objects.requireNonNull(
                        await(inputStreamFactory.openAsync(upload.getStartOffset())), "inputStreamFactory result");
                positionInputStream();
                trackingStream = new PositionTrackingInputStream(
                        inputStream, determinePosition(inputStream, upload.getStartOffset()));

                updateState(TransferStates.IN_PROGRESS);
                updateProgress(upload.getStartOffset());
                writeAndAwaitDisconnectRace();
                linger();

                updateProgress(currentStreamPosition());
                updateState(TransferStates.COMPLETED.or(TransferStates.SUCCEEDED));
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
                    (sender, eventArgs) -> updateProgress(upload.getStartOffset() + eventArgs.getCurrentLength());
            disconnectedListener = (sender, eventArgs) -> {
                Throwable failure = eventArgs.getException();
                if (failure instanceof CancellationException || failure instanceof TimeoutException) {
                    disconnected.completeExceptionally(failure);
                } else {
                    disconnected.completeExceptionally(
                            new ConnectionException("Transfer failed: " + eventArgs.getMessage(), failure));
                }
            };
            connection.addDataWrittenListener(dataWrittenListener);
            connection.addDisconnectedListener(disconnectedListener);
        }

        private void readStartOffset() {
            try {
                byte[] bytes = await(connection.readAsync(8, cancellationToken));
                if (bytes.length != 8) {
                    throw new IOException("Expected 8 bytes but received " + bytes.length);
                }
                upload.setStartOffset(
                        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong());
            } catch (Throwable failure) {
                Throwable cause = unwrap(failure);
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
                                            Math.min(requestedBytes, granted), cancellationToken)),
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
                            cancellationToken);
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
                while (!cancellationToken.isCancellationRequested()) {
                    long remainingNanos = deadline - System.nanoTime();
                    if (remainingNanos <= 0) {
                        connection.disconnect("Transfer complete, maximum linger " + "time exceeded");
                        return;
                    }
                    long remainingMillis = Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
                    try {
                        await(connection
                                .readAsync(1, cancellationToken)
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
                cancellationToken.throwIfCancellationRequested();
            } catch (Throwable failure) {
                if (!(unwrap(failure) instanceof ConnectionReadException)) {
                    throw failure;
                }
            }
        }

        private void handleFailure(Throwable failure) {
            if (failure instanceof TransferRejectedException) {
                upload.setException(failure);
                updateState(TransferStates.COMPLETED.or(TransferStates.REJECTED));
                return;
            }
            if (failure instanceof CancellationException) {
                disconnectTransfer("Transfer cancelled", failure);
                upload.setException(failure);
                updateProgress(currentStreamPosition());
                updateState(TransferStates.COMPLETED.or(TransferStates.CANCELLED));
                return;
            }
            if (failure instanceof TimeoutException) {
                disconnectTransfer("Transfer timed out", failure);
                upload.setException(failure);
                updateProgress(currentStreamPosition());
                updateState(TransferStates.COMPLETED.or(TransferStates.TIMED_OUT));
                return;
            }
            disconnectTransfer("Transfer error", failure);
            upload.setException(failure);
            updateProgress(currentStreamPosition());
            updateState(TransferStates.COMPLETED.or(TransferStates.ERRORED));
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
                if (!upload.getState().hasFlag(TransferStates.SUCCEEDED)) {
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
                        await(getUserEndPointAsync(upload.getUsername(), CancellationToken.none()));
                MessageConnection messageConnection = await(peerConnectionManager.getOrAddMessageConnectionAsync(
                        upload.getUsername(), currentEndpoint, CancellationToken.none()));
                OutgoingMessage message = upload.getState().hasFlag(TransferStates.CANCELLED)
                        ? new UploadDenied(upload.getFilename(), "Cancelled")
                        : new UploadFailed(upload.getFilename());
                await(invokeMessageWrite(messageConnection, message, CancellationToken.none()));
            } catch (Throwable ignored) {
                // Failure notification is intentionally best effort.
            }
        }

        private void releasePermits() {
            if (perUserPermit.compareAndSet(true, false)) {
                perUserSemaphore.release();
            }
            if (slot.compareAndSet(true, false) && transferOptions.getSlotReleased() != null) {
                try {
                    Thread.sleep(10);
                    transferOptions.getSlotReleased().onSlotReleased(upload.toTransfer());
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                } catch (Throwable ignored) {
                    // Slot-release callbacks cannot block cleanup.
                }
            }
            if (globalPermit.compareAndSet(true, false)) {
                globalUploadSemaphore.release();
            }
        }

        private void updateState(TransferStates state) {
            upload.setState(state);
            Transfer transfer = upload.toTransfer();
            TransferStateChangedEventArgs eventArgs = new TransferStateChangedEventArgs(lastState, transfer);
            TransferStates previous = lastState;
            lastState = state;
            if (transferOptions.getStateChanged() != null) {
                transferOptions.getStateChanged().onStateChanged(new TransferStateChange(previous, transfer));
            }
            raise(Event.TRANSFER_STATE_CHANGED, eventArgs);
        }

        private void updateProgress(long bytesUploaded) {
            long previous = upload.getBytesTransferred();
            upload.updateProgress(bytesUploaded);
            Transfer transfer = upload.toTransfer();
            if (transferOptions.getProgressUpdated() != null) {
                transferOptions.getProgressUpdated().onProgressUpdated(new TransferProgressUpdate(previous, transfer));
            }
            raise(Event.TRANSFER_PROGRESS_UPDATED, new TransferProgressUpdatedEventArgs(previous, transfer));
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

    private static CompletableFuture<Void> acquirePermit(Semaphore semaphore, CancellationToken cancellationToken) {
        try {
            cancellationToken.throwIfCancellationRequested();
            if (semaphore.tryAcquire()) {
                return CompletableFuture.completedFuture(null);
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        CancellationRegistration registration = cancellationToken.register(
                () -> result.completeExceptionally(new CancellationException("The operation was cancelled")));
        CompletableFuture.runAsync(() -> {
            try {
                while (!result.isDone()) {
                    cancellationToken.throwIfCancellationRequested();
                    if (semaphore.tryAcquire(50, TimeUnit.MILLISECONDS)) {
                        if (!result.complete(null)) {
                            semaphore.release();
                        }
                        return;
                    }
                }
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                result.completeExceptionally(new CancellationException("The operation was interrupted"));
            } catch (Throwable failure) {
                result.completeExceptionally(failure);
            }
        });
        result.whenComplete((ignored, failure) -> registration.close());
        return result;
    }

    private static byte[] buildSearchMessage(SearchScope scope, SearchInternal search) {
        String text = search.getQuery().getSearchText();
        return switch (scope.getType()) {
            case ROOM ->
                new RoomSearchRequest(scope.getSubjects().iterator().next(), text, search.getToken()).toByteArray();
            case USER -> {
                ByteArrayOutputStream messages = new ByteArrayOutputStream();
                for (String subject : scope.getSubjects()) {
                    messages.writeBytes(new UserSearchRequest(subject, text, search.getToken()).toByteArray());
                }
                yield messages.toByteArray();
            }
            case WISHLIST -> new WishlistSearchRequest(text, search.getToken()).toByteArray();
            case NETWORK -> new SearchRequest(text, search.getToken()).toByteArray();
        };
    }

    private CompletableFuture<Void> invokeServerByteWrite(byte[] message, CancellationToken cancellationToken) {
        try {
            return serverConnection.writeAsync(message, defaultToken(cancellationToken));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<Void> writeServerAsync(
            OutgoingMessage message, CancellationToken cancellationToken, String failurePrefix) {
        return mapClientFailure(invokeServerWrite(message, cancellationToken), failurePrefix);
    }

    private CompletableFuture<Void> executeCorrelatedServerCommand(
            OutgoingMessage message, WaitKey waitKey, CancellationToken cancellationToken, String failurePrefix) {
        CancellationToken token = defaultToken(cancellationToken);
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
            CancellationToken cancellationToken,
            String failurePrefix,
            Class<? extends Throwable>... preservedFailures) {
        CancellationToken token = defaultToken(cancellationToken);
        CompletableFuture<T> wait;
        try {
            wait = waiter.waitAsync(waitKey, resultType, null, token);
        } catch (Throwable failure) {
            return mapClientFailure(CompletableFuture.failedFuture(failure), failurePrefix, preservedFailures);
        }
        CompletableFuture<T> operation = invokeServerWrite(message, token).thenCompose(ignored -> wait);
        return mapClientFailure(operation, failurePrefix, preservedFailures);
    }

    private CompletableFuture<Void> invokeServerWrite(OutgoingMessage message, CancellationToken cancellationToken) {
        return invokeMessageWrite(serverConnection, message, cancellationToken);
    }

    private static CompletableFuture<Void> invokeMessageWrite(
            MessageConnection connection, OutgoingMessage message, CancellationToken cancellationToken) {
        CompletableFuture<Void> operation;
        try {
            operation = connection.writeAsync(message, defaultToken(cancellationToken));
        } catch (Throwable failure) {
            operation = CompletableFuture.failedFuture(failure);
        }
        return operation;
    }

    private CompletableFuture<InetSocketAddress> retrieveUserEndPoint(
            String requestedUsername, CancellationToken cancellationToken, IUserEndPointCache cache) {
        CompletableFuture<UserAddressResponse> wait;
        try {
            wait = waiter.waitAsync(
                    new dev.slsk.common.WaitKey(MessageCode.Server.GET_PEER_ADDRESS, requestedUsername),
                    UserAddressResponse.class,
                    null,
                    cancellationToken);
        } catch (Throwable failure) {
            return mapUserEndPointFailure(CompletableFuture.failedFuture(failure), requestedUsername);
        }
        CompletableFuture<InetSocketAddress> operation = invokeServerWrite(
                        new UserAddressRequest(requestedUsername), cancellationToken)
                .thenCompose(ignored -> wait)
                .thenApply(response -> {
                    if (response.getIpAddress().isAnyLocalAddress()) {
                        throw new UserOfflineException("User " + requestedUsername + " appears to be offline");
                    }
                    InetSocketAddress result = response.getIpEndPoint();
                    if (cache != null) {
                        try {
                            cache.addOrUpdate(requestedUsername, result);
                        } catch (Throwable failure) {
                            throw new UserEndPointCacheException(
                                    "Exception retrieving or updating user "
                                            + "endpoint cache: "
                                            + failureMessage(failure),
                                    failure);
                        }
                        diagnostic.debug("EndPoint cache MISS for " + requestedUsername + ": " + result);
                    }
                    return result;
                });
        return mapUserEndPointFailure(operation, requestedUsername);
    }

    private static CompletableFuture<InetSocketAddress> mapUserEndPointFailure(
            CompletableFuture<InetSocketAddress> operation, String requestedUsername) {
        return operation.handle((result, failure) -> {
            if (failure == null) {
                return result;
            }
            Throwable cause = unwrap(failure);
            if (cause instanceof UserOfflineException
                    || cause instanceof UserEndPointCacheException
                    || cause instanceof CancellationException
                    || cause instanceof TimeoutException) {
                throw new CompletionException(cause);
            }
            throw new CompletionException(new UserEndPointException(
                    "Failed to retrieve endpoint for user " + requestedUsername + ": " + failureMessage(cause), cause));
        });
    }

    private static CacheLookupResult<InetSocketAddress> tryCacheGet(
            IUserEndPointCache cache, String requestedUsername) {
        try {
            return cache.tryGet(requestedUsername);
        } catch (Throwable failure) {
            throw new UserEndPointCacheException(
                    "Exception retrieving or updating user endpoint cache: " + failureMessage(failure), failure);
        }
    }

    private static CancellationToken defaultToken(CancellationToken token) {
        return token == null ? CancellationToken.none() : token;
    }

    private static <T> CompletableFuture<T> mapClientFailure(
            CompletableFuture<T> operation, String prefix, Class<? extends Throwable>... preservedFailures) {
        return operation.handle((result, failure) -> {
            if (failure == null) {
                return result;
            }
            Throwable cause = unwrap(failure);
            if (cause instanceof CancellationException || cause instanceof TimeoutException) {
                throw new CompletionException(cause);
            }
            for (Class<? extends Throwable> preserved : preservedFailures) {
                if (preserved.isInstance(cause)) {
                    throw new CompletionException(cause);
                }
            }
            throw new CompletionException(new SoulseekClientException(prefix + failureMessage(cause), cause));
        });
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private record SearchInvocation(SearchQuery query, SearchScope scope, int token, SearchOptions options) {}

    @FunctionalInterface
    interface ClientListenerFactory {
        Listener create(InetAddress ipAddress, int port, ConnectionOptions connectionOptions);
    }

    private enum Event {
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
