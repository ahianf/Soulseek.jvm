// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.common.IOAdapter;
import dev.slsk.common.IWaiter;
import dev.slsk.common.TokenBucket;
import dev.slsk.common.TokenFactory;
import dev.slsk.common.WaitKey;
import dev.slsk.common.Waiter;
import dev.slsk.diagnostics.DiagnosticEventArgs;
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
import dev.slsk.exceptions.KickedFromServerException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferReportedFailedException;
import dev.slsk.exceptions.UserEndPointCacheException;
import dev.slsk.exceptions.UserEndPointException;
import dev.slsk.exceptions.UserNotFoundException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.handlers.DistributedMessageHandler;
import dev.slsk.messaging.handlers.DistributedMessageHandlerClient;
import dev.slsk.messaging.handlers.IDistributedMessageHandler;
import dev.slsk.messaging.handlers.IPeerMessageHandler;
import dev.slsk.messaging.handlers.IServerMessageHandler;
import dev.slsk.messaging.handlers.PeerMessageHandler;
import dev.slsk.messaging.handlers.PeerMessageHandlerClient;
import dev.slsk.messaging.handlers.ServerMessageEvent;
import dev.slsk.messaging.handlers.ServerMessageHandler;
import dev.slsk.messaging.handlers.ServerMessageHandlerClient;
import dev.slsk.messaging.messages.AcknowledgePrivateMessageCommand;
import dev.slsk.messaging.messages.AcknowledgePrivilegeNotificationCommand;
import dev.slsk.messaging.messages.CheckPrivilegesRequest;
import dev.slsk.messaging.messages.GivePrivilegesCommand;
import dev.slsk.messaging.messages.IOutgoingMessage;
import dev.slsk.messaging.messages.NewPassword;
import dev.slsk.messaging.messages.PrivateMessageCommand;
import dev.slsk.messaging.messages.PrivateRoomAddOperator;
import dev.slsk.messaging.messages.PrivateRoomAddUser;
import dev.slsk.messaging.messages.PrivateRoomDropMembershipCommand;
import dev.slsk.messaging.messages.PrivateRoomDropOwnershipCommand;
import dev.slsk.messaging.messages.PrivateRoomRemoveOperator;
import dev.slsk.messaging.messages.PrivateRoomRemoveUser;
import dev.slsk.messaging.messages.RoomMessageCommand;
import dev.slsk.messaging.messages.SendUploadSpeedCommand;
import dev.slsk.messaging.messages.ServerPing;
import dev.slsk.messaging.messages.SetOnlineStatusCommand;
import dev.slsk.messaging.messages.SetRoomTickerCommand;
import dev.slsk.messaging.messages.SetSharedCountsCommand;
import dev.slsk.messaging.messages.StartPublicChatCommand;
import dev.slsk.messaging.messages.StopPublicChatCommand;
import dev.slsk.messaging.messages.UnwatchUserCommand;
import dev.slsk.messaging.messages.UserAddressRequest;
import dev.slsk.messaging.messages.UserAddressResponse;
import dev.slsk.messaging.messages.UserPrivilegesRequest;
import dev.slsk.messaging.messages.UserStatisticsRequest;
import dev.slsk.messaging.messages.UserStatusRequest;
import dev.slsk.messaging.messages.WatchUserRequest;
import dev.slsk.messaging.messages.WatchUserResponse;
import dev.slsk.network.ConnectionFactory;
import dev.slsk.network.DistributedConnectionManager;
import dev.slsk.network.DistributedConnectionManagerClient;
import dev.slsk.network.IConnectionFactory;
import dev.slsk.network.IDistributedConnectionManager;
import dev.slsk.network.IListenerHandler;
import dev.slsk.network.IMessageConnection;
import dev.slsk.network.IPeerConnectionManager;
import dev.slsk.network.ListenerHandler;
import dev.slsk.network.ListenerHandlerClient;
import dev.slsk.network.PeerConnectionManager;
import dev.slsk.network.PeerConnectionManagerClient;
import dev.slsk.network.PeerEndpoint;
import dev.slsk.network.tcp.IListener;
import dev.slsk.options.SoulseekClientOptions;
import dev.slsk.search.ISearchResponder;
import dev.slsk.search.SearchInternal;
import dev.slsk.search.SearchResponder;
import dev.slsk.search.SearchResponderClient;
import dev.slsk.transfer.TransferInternal;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A client for the Soulseek file-sharing network.
 */
public class SoulseekClient
        implements AutoCloseable,
                DistributedConnectionManagerClient,
                DistributedMessageHandlerClient,
                ListenerHandlerClient,
                PeerConnectionManagerClient,
                PeerMessageHandlerClient,
                SearchResponderClient,
                ServerMessageHandlerClient {

    private static final int MAJOR_VERSION = 170;
    private static volatile boolean raiseEventsAsynchronously;

    private final SoulseekClientOptions options;
    private final int minorVersion;
    private final IWaiter waiter;
    private final TokenFactory tokenFactory;
    private final IOAdapter ioAdapter;
    private final TokenBucket uploadTokenBucket;
    private final TokenBucket downloadTokenBucket;
    private final IConnectionFactory connectionFactory;
    private final IListenerHandler listenerHandler;
    private final ISearchResponder searchResponder;
    private final IPeerMessageHandler peerMessageHandler;
    private final IDistributedMessageHandler distributedMessageHandler;
    private final IPeerConnectionManager peerConnectionManager;
    private final IDistributedConnectionManager distributedConnectionManager;
    private final IServerMessageHandler serverMessageHandler;
    private final IDiagnosticFactory diagnostic;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ScheduledExecutorService cleanupScheduler;
    private final Map<Event, CopyOnWriteArrayList<SoulseekClientEventListener<?>>> listeners =
            new EnumMap<>(Event.class);

    private volatile IMessageConnection serverConnection;
    private volatile IListener listener;
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
            IMessageConnection serverConnection,
            IConnectionFactory connectionFactory,
            IPeerConnectionManager peerConnectionManager,
            IDistributedConnectionManager distributedConnectionManager,
            IServerMessageHandler serverMessageHandler,
            IPeerMessageHandler peerMessageHandler,
            IDistributedMessageHandler distributedMessageHandler,
            IListener listener,
            IListenerHandler listenerHandler,
            ISearchResponder searchResponder,
            IWaiter waiter,
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
        this.waiter = waiter == null ? new Waiter(this.options.getMessageTimeout()) : waiter;
        this.tokenFactory = tokenFactory == null ? new TokenFactory(this.options.getStartingToken()) : tokenFactory;
        this.ioAdapter = ioAdapter == null ? new IOAdapter() : ioAdapter;
        this.uploadTokenBucket = uploadTokenBucket == null
                ? new TokenBucket((this.options.getMaximumUploadSpeed() * 1024L) / 10, 100)
                : uploadTokenBucket;
        this.downloadTokenBucket = downloadTokenBucket == null
                ? new TokenBucket((this.options.getMaximumDownloadSpeed() * 1024L) / 10, 100)
                : downloadTokenBucket;
        this.connectionFactory = connectionFactory == null ? new ConnectionFactory() : connectionFactory;
        for (Event event : Event.values()) {
            listeners.put(event, new CopyOnWriteArrayList<>());
        }

        diagnostic = diagnosticFactory == null
                ? new DiagnosticFactory(
                        this.options.getMinimumDiagnosticLevel(),
                        eventArgs -> raise(Event.DIAGNOSTIC_GENERATED, eventArgs))
                : diagnosticFactory;
        GlobalDiagnostic.init(diagnostic);

        this.listenerHandler = listenerHandler == null ? new ListenerHandler(this) : listenerHandler;
        this.searchResponder = searchResponder == null ? new SearchResponder(this) : searchResponder;
        this.peerMessageHandler = peerMessageHandler == null ? new PeerMessageHandler(this) : peerMessageHandler;
        this.distributedMessageHandler =
                distributedMessageHandler == null ? new DistributedMessageHandler(this) : distributedMessageHandler;
        this.peerConnectionManager =
                peerConnectionManager == null ? new PeerConnectionManager(this) : peerConnectionManager;
        this.distributedConnectionManager = distributedConnectionManager == null
                ? new DistributedConnectionManager(this)
                : distributedConnectionManager;
        this.serverMessageHandler =
                serverMessageHandler == null ? new ServerMessageHandler(this) : serverMessageHandler;

        bindEvents();
        cleanupScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "soulseek-client-cleanup");
            thread.setDaemon(true);
            return thread;
        });
        cleanupScheduler.scheduleAtFixedRate(() -> {}, 5, 5, TimeUnit.MINUTES);
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

    public final void addDiagnosticGeneratedListener(SoulseekClientEventListener<DiagnosticEventArgs> value) {
        add(Event.DIAGNOSTIC_GENERATED, value);
    }

    public final void removeDiagnosticGeneratedListener(SoulseekClientEventListener<DiagnosticEventArgs> value) {
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
        return endpointRequests.computeIfAbsent(
                requestedUsername,
                ignored -> retrieveUserEndPoint(requestedUsername, token, cache)
                        .whenComplete((result, failure) -> endpointRequests.remove(requestedUsername)));
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
    public final IWaiter getWaiter() {
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
    public final IPeerConnectionManager getPeerConnectionManager() {
        return peerConnectionManager;
    }

    @Override
    public final IDistributedConnectionManager getDistributedConnectionManager() {
        return distributedConnectionManager;
    }

    @Override
    public final IDistributedMessageHandler getDistributedMessageHandler() {
        return distributedMessageHandler;
    }

    @Override
    public final ISearchResponder getSearchResponder() {
        return searchResponder;
    }

    @Override
    public final IMessageConnection getServerConnection() {
        return serverConnection;
    }

    @Override
    public final IPeerMessageHandler getPeerMessageHandler() {
        return peerMessageHandler;
    }

    @Override
    public final IListener getListener() {
        return listener;
    }

    final IServerMessageHandler getServerMessageHandler() {
        return serverMessageHandler;
    }

    final IListenerHandler getListenerHandler() {
        return listenerHandler;
    }

    final IConnectionFactory getConnectionFactory() {
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

    void setStateForTest(SoulseekClientStates value) {
        state = value;
    }

    void setServerConnectionForTest(IMessageConnection value) {
        serverConnection = value;
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

    private CompletableFuture<Void> writeServerAsync(
            IOutgoingMessage message, CancellationToken cancellationToken, String failurePrefix) {
        return mapClientFailure(invokeServerWrite(message, cancellationToken), failurePrefix);
    }

    private CompletableFuture<Void> executeCorrelatedServerCommand(
            IOutgoingMessage message, WaitKey waitKey, CancellationToken cancellationToken, String failurePrefix) {
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
            IOutgoingMessage message,
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

    private CompletableFuture<Void> invokeServerWrite(IOutgoingMessage message, CancellationToken cancellationToken) {
        CompletableFuture<Void> operation;
        try {
            operation = serverConnection.writeAsync(message, defaultToken(cancellationToken));
        } catch (Throwable failure) {
            operation = CompletableFuture.failedFuture(failure);
        }
        return operation;
    }

    private CompletableFuture<InetSocketAddress> retrieveUserEndPoint(
            String requestedUsername, CancellationToken cancellationToken, IUserEndPointCache cache) {
        CompletableFuture<UserAddressResponse> wait = waiter.waitAsync(
                new dev.slsk.common.WaitKey(MessageCode.Server.GET_PEER_ADDRESS, requestedUsername),
                UserAddressResponse.class,
                null,
                cancellationToken);
        CompletableFuture<InetSocketAddress> operation = serverConnection
                .writeAsync(new UserAddressRequest(requestedUsername), cancellationToken)
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
