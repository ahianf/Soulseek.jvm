// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.RoomJoinForbiddenException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.internal.ServerLink;
import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.NetworkExecutor;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.connection.ServerInfo;
import dev.slsk.internal.diagnostics.DiagnosticEvent;
import dev.slsk.internal.diagnostics.DiagnosticEventListener;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.diagnostics.FilteringDiagnosticSink;
import dev.slsk.internal.events.PrivateMessageReceivedEvent;
import dev.slsk.internal.events.PrivilegeNotificationReceivedEvent;
import dev.slsk.internal.events.PublicChatMessageReceivedEvent;
import dev.slsk.internal.events.RoomJoinedEvent;
import dev.slsk.internal.events.RoomLeftEvent;
import dev.slsk.internal.events.RoomMessageReceivedEvent;
import dev.slsk.internal.events.RoomTickerAddedEvent;
import dev.slsk.internal.events.RoomTickerListReceivedEvent;
import dev.slsk.internal.events.RoomTickerRemovedEvent;
import dev.slsk.internal.events.UserCannotConnectEvent;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.messaging.messages.CannotConnect;
import dev.slsk.internal.messaging.messages.CannotJoinRoomNotification;
import dev.slsk.internal.messaging.messages.ConnectToPeerResponse;
import dev.slsk.internal.messaging.messages.ExcludedSearchPhrasesNotification;
import dev.slsk.internal.messaging.messages.GlobalMessageNotification;
import dev.slsk.internal.messaging.messages.IntegerResponse;
import dev.slsk.internal.messaging.messages.JoinRoomResponse;
import dev.slsk.internal.messaging.messages.LeaveRoomResponse;
import dev.slsk.internal.messaging.messages.LoginResponse;
import dev.slsk.internal.messaging.messages.NetInfoNotification;
import dev.slsk.internal.messaging.messages.NewPassword;
import dev.slsk.internal.messaging.messages.PrivateMessageNotification;
import dev.slsk.internal.messaging.messages.PrivateRoomAddOperator;
import dev.slsk.internal.messaging.messages.PrivateRoomAddUser;
import dev.slsk.internal.messaging.messages.PrivateRoomOwnedListNotification;
import dev.slsk.internal.messaging.messages.PrivateRoomRemoveOperator;
import dev.slsk.internal.messaging.messages.PrivateRoomRemoveUser;
import dev.slsk.internal.messaging.messages.PrivateRoomToggle;
import dev.slsk.internal.messaging.messages.PrivateRoomUserListNotification;
import dev.slsk.internal.messaging.messages.PrivilegeNotification;
import dev.slsk.internal.messaging.messages.PrivilegedUserListNotification;
import dev.slsk.internal.messaging.messages.PrivilegedUserNotification;
import dev.slsk.internal.messaging.messages.PublicChatMessageNotification;
import dev.slsk.internal.messaging.messages.RoomListResponseFactory;
import dev.slsk.internal.messaging.messages.RoomMessageNotification;
import dev.slsk.internal.messaging.messages.RoomTickerAddedNotification;
import dev.slsk.internal.messaging.messages.RoomTickerListNotification;
import dev.slsk.internal.messaging.messages.RoomTickerRemovedNotification;
import dev.slsk.internal.messaging.messages.ServerSearchRequest;
import dev.slsk.internal.messaging.messages.StringResponse;
import dev.slsk.internal.messaging.messages.UserAddressResponse;
import dev.slsk.internal.messaging.messages.UserJoinedRoomNotification;
import dev.slsk.internal.messaging.messages.UserLeftRoomNotification;
import dev.slsk.internal.messaging.messages.UserPrivilegeResponse;
import dev.slsk.internal.messaging.messages.UserStatisticsResponseFactory;
import dev.slsk.internal.messaging.messages.UserStatusResponseFactory;
import dev.slsk.internal.messaging.messages.WatchUserResponse;
import dev.slsk.internal.network.DistributedConnectionManager;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.MessageEvent;
import dev.slsk.internal.network.PeerConnectionManager;
import dev.slsk.internal.network.PeerEndpoint;
import dev.slsk.internal.network.TransferConnectionResult;
import dev.slsk.internal.network.tcp.Connection;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.room.RoomData;
import dev.slsk.internal.room.RoomInfo;
import dev.slsk.internal.room.RoomList;
import dev.slsk.internal.search.SearchInternal;
import dev.slsk.internal.search.SearchResponder;
import dev.slsk.internal.search.SearchScopeType;
import dev.slsk.internal.transfer.TransferInternal;
import dev.slsk.internal.user.UserStatistics;
import dev.slsk.internal.user.UserStatus;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

/** Handles incoming messages from the server connection. */
public final class DefaultServerMessageHandler implements ServerMessageHandler {
    private final Supplier<SoulseekClientOptions> options;
    private final ServerLink server;
    private final Waiter waiter;
    private final Supplier<Map<Integer, SearchInternal>> searches;
    private final Supplier<Map<Integer, TransferInternal>> downloads;
    private final Supplier<PeerConnectionManager> peers;
    private final Supplier<DistributedConnectionManager> distributed;
    private final Supplier<DistributedMessageHandler> distributedMessages;
    private final Supplier<SearchResponder> searchResponses;
    private final DiagnosticSink diagnostic;
    private final CopyOnWriteArrayList<DiagnosticEventListener> diagnosticListeners = new CopyOnWriteArrayList<>();
    private final Map<ServerMessageEvent, CopyOnWriteArrayList<ServerMessageHandlerEventListener<?>>> listeners =
            new EnumMap<>(ServerMessageEvent.class);

    /** Creates a handler with its default diagnostic factory. */
    public DefaultServerMessageHandler(
            Supplier<SoulseekClientOptions> options,
            ServerLink server,
            Waiter waiter,
            Supplier<Map<Integer, SearchInternal>> searches,
            Supplier<Map<Integer, TransferInternal>> downloads,
            Supplier<PeerConnectionManager> peers,
            Supplier<DistributedConnectionManager> distributed,
            Supplier<DistributedMessageHandler> distributedMessages,
            Supplier<SearchResponder> searchResponses) {
        this(
                options,
                server,
                waiter,
                searches,
                downloads,
                peers,
                distributed,
                distributedMessages,
                searchResponses,
                null);
    }

    /** Creates a handler. */
    public DefaultServerMessageHandler(
            Supplier<SoulseekClientOptions> options,
            ServerLink server,
            Waiter waiter,
            Supplier<Map<Integer, SearchInternal>> searches,
            Supplier<Map<Integer, TransferInternal>> downloads,
            Supplier<PeerConnectionManager> peers,
            Supplier<DistributedConnectionManager> distributed,
            Supplier<DistributedMessageHandler> distributedMessages,
            Supplier<SearchResponder> searchResponses,
            DiagnosticSink diagnosticFactory) {
        this.options = Objects.requireNonNull(options, "options");
        this.server = Objects.requireNonNull(server, "server");
        this.waiter = Objects.requireNonNull(waiter, "waiter");
        this.searches = Objects.requireNonNull(searches, "searches");
        this.downloads = Objects.requireNonNull(downloads, "downloads");
        this.peers = Objects.requireNonNull(peers, "peers");
        this.distributed = Objects.requireNonNull(distributed, "distributed");
        this.distributedMessages = Objects.requireNonNull(distributedMessages, "distributedMessages");
        this.searchResponses = Objects.requireNonNull(searchResponses, "searchResponses");
        diagnostic = diagnosticFactory == null
                ? new FilteringDiagnosticSink(options.get().getMinimumDiagnosticLevel(), this::raiseDiagnostic)
                : diagnosticFactory;
        for (ServerMessageEvent event : ServerMessageEvent.values()) {
            listeners.put(event, new CopyOnWriteArrayList<>());
        }
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
    public <T> void addListener(ServerMessageEvent event, ServerMessageHandlerEventListener<T> listener) {
        listeners.get(Objects.requireNonNull(event, "event")).add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public <T> void removeListener(ServerMessageEvent event, ServerMessageHandlerEventListener<T> listener) {
        listeners.get(Objects.requireNonNull(event, "event")).remove(listener);
    }

    @Override
    public void handleMessageRead(MessageConnection sender, MessageEvent eventData) {
        handleMessageRead(sender, eventData.getMessage());
    }

    @Override
    public void handleMessageRead(MessageConnection sender, byte[] message) {
        MessageCode.Server code;
        try {
            code = new MessageReader<>(message, MessageCode.Server.class).readCode();
        } catch (IllegalArgumentException unknown) {
            // A code outside the table is a protocol addition or a newer
            // server, not a broken connection. The C# source parses it
            // tolerantly and lands in the switch's default; throwing here
            // instead killed the read loop — for this connection, the whole
            // client.
            diagnostic.debug("Ignored an unknown server message: " + unknown.getMessage());
            return;
        }
        if (code != MessageCode.Server.EMBEDDED_MESSAGE) {
            diagnostic.debug("Server message received: " + code);
        }

        try {
            switch (code) {
                case PARENT_MIN_SPEED -> {
                    raise(ServerMessageEvent.SERVER_INFO_RECEIVED, new ServerInfo(integer(message)));
                }
                case PARENT_SPEED_RATIO -> {
                    raise(ServerMessageEvent.SERVER_INFO_RECEIVED, new ServerInfo(null, integer(message)));
                }
                case WISHLIST_INTERVAL -> {
                    raise(ServerMessageEvent.SERVER_INFO_RECEIVED, new ServerInfo(null, null, integer(message)));
                }
                case CHECK_PRIVILEGES -> {
                    waiter.complete(new WaitKey(code), integer(message));
                }
                case PRIVATE_ROOM_ADDED -> {
                    raise(ServerMessageEvent.PRIVATE_ROOM_MEMBERSHIP_ADDED, string(message));
                }
                case PRIVATE_ROOM_REMOVED -> {
                    String room = string(message);
                    waiter.complete(new WaitKey(code, room));
                    raise(ServerMessageEvent.PRIVATE_ROOM_MEMBERSHIP_REMOVED, room);
                }
                case PRIVATE_ROOM_OPERATOR_ADDED -> {
                    raise(ServerMessageEvent.PRIVATE_ROOM_MODERATION_ADDED, string(message));
                }
                case PRIVATE_ROOM_OPERATOR_REMOVED -> {
                    String room = string(message);
                    waiter.complete(new WaitKey(code, room));
                    raise(ServerMessageEvent.PRIVATE_ROOM_MODERATION_REMOVED, room);
                }
                case NEW_PASSWORD -> {
                    waiter.complete(
                            new WaitKey(code),
                            NewPassword.fromByteArray(message).getPassword());
                }
                case PRIVATE_ROOM_TOGGLE -> {
                    waiter.complete(
                            new WaitKey(code),
                            PrivateRoomToggle.fromByteArray(message).isAcceptInvitations());
                }
                case EXCLUDED_SEARCH_PHRASES -> {
                    raise(
                            ServerMessageEvent.EXCLUDED_SEARCH_PHRASES_RECEIVED,
                            ExcludedSearchPhrasesNotification.fromByteArray(message));
                }
                case GLOBAL_ADMIN_MESSAGE -> {
                    raise(ServerMessageEvent.GLOBAL_MESSAGE_RECEIVED, GlobalMessageNotification.fromByteArray(message));
                }
                case PING -> {
                    waiter.complete(new WaitKey(code));
                }
                case LOGIN -> {
                    waiter.complete(new WaitKey(code), LoginResponse.fromByteArray(message));
                }
                case ROOM_LIST -> {
                    RoomList rooms = RoomListResponseFactory.fromByteArray(message);
                    waiter.complete(new WaitKey(code), rooms);
                    raise(ServerMessageEvent.ROOM_LIST_RECEIVED, rooms);
                }
                case PRIVATE_ROOM_OWNED -> {
                    RoomInfo room = PrivateRoomOwnedListNotification.fromByteArray(message);
                    raise(ServerMessageEvent.PRIVATE_ROOM_MODERATED_USER_LIST_RECEIVED, room);
                }
                case PRIVATE_ROOM_USERS -> {
                    RoomInfo room = PrivateRoomUserListNotification.fromByteArray(message);
                    raise(ServerMessageEvent.PRIVATE_ROOM_USER_LIST_RECEIVED, room);
                }
                case PRIVILEGED_USERS -> {
                    raise(
                            ServerMessageEvent.PRIVILEGED_USER_LIST_RECEIVED,
                            PrivilegedUserListNotification.fromByteArray(message));
                }
                case ADD_PRIVILEGED_USER -> {
                    raise(
                            ServerMessageEvent.PRIVILEGE_NOTIFICATION_RECEIVED,
                            new PrivilegeNotificationReceivedEvent(PrivilegedUserNotification.fromByteArray(message)));
                }
                case NOTIFY_PRIVILEGES -> handlePrivilegeNotification(message);
                case USER_PRIVILEGES -> {
                    UserPrivilegeResponse response = UserPrivilegeResponse.fromByteArray(message);
                    waiter.complete(new WaitKey(code, response.getUsername()), response.isPrivileged());
                }
                case NET_INFO -> handleNetInfo(message);
                case DISTRIBUTED_RESET -> {
                    diagnostic.info("Distributed network reset received from the server");
                    raise(ServerMessageEvent.DISTRIBUTED_NETWORK_RESET, null);
                    distributed.get().removeAndDisposeAll();
                    distributed.get().resetStatus();
                }
                case CANNOT_CONNECT -> {
                    handleCannotConnect(message);
                }
                case CANNOT_JOIN_ROOM -> {
                    CannotJoinRoomNotification rejected = CannotJoinRoomNotification.fromByteArray(message);
                    waiter.fail(
                            new WaitKey(MessageCode.Server.JOIN_ROOM, rejected.getRoomName()),
                            new RoomJoinForbiddenException(
                                    "The server rejected the request to join room " + rejected.getRoomName()));
                }
                case CONNECT_TO_PEER -> handleConnectToPeer(message);
                case WATCH_USER -> {
                    WatchUserResponse response = WatchUserResponse.fromByteArray(message);
                    waiter.complete(new WaitKey(code, response.getUsername()), response);
                }
                case GET_STATUS -> {
                    UserStatus status = UserStatusResponseFactory.fromByteArray(message);
                    waiter.complete(new WaitKey(code, status.getUsername()), status);
                    raise(ServerMessageEvent.USER_STATUS_CHANGED, status);
                }
                case GET_USER_STATS -> {
                    UserStatistics statistics = UserStatisticsResponseFactory.fromByteArray(message);
                    waiter.complete(new WaitKey(code, statistics.getUsername()), statistics);
                    raise(ServerMessageEvent.USER_STATISTICS_CHANGED, statistics);
                }
                case PRIVATE_MESSAGE -> handlePrivateMessage(message);
                case GET_PEER_ADDRESS -> {
                    UserAddressResponse response = UserAddressResponse.fromByteArray(message);
                    waiter.complete(new WaitKey(code, response.getUsername()), response);
                }
                case JOIN_ROOM -> {
                    RoomData response = JoinRoomResponse.fromByteArray(message);
                    waiter.complete(new WaitKey(code, response.getName()), response);
                }
                case LEAVE_ROOM -> {
                    LeaveRoomResponse response = LeaveRoomResponse.fromByteArray(message);
                    waiter.complete(new WaitKey(code, response.getRoomName()));
                    raise(ServerMessageEvent.ROOM_LEFT, new RoomLeftEvent(response.getRoomName(), server.username()));
                }
                case SAY_IN_CHAT_ROOM -> {
                    raise(
                            ServerMessageEvent.ROOM_MESSAGE_RECEIVED,
                            new RoomMessageReceivedEvent(RoomMessageNotification.fromByteArray(message)));
                }
                case PUBLIC_CHAT -> {
                    raise(
                            ServerMessageEvent.PUBLIC_CHAT_MESSAGE_RECEIVED,
                            new PublicChatMessageReceivedEvent(PublicChatMessageNotification.fromByteArray(message)));
                }
                case USER_JOINED_ROOM -> {
                    raise(
                            ServerMessageEvent.ROOM_JOINED,
                            new RoomJoinedEvent(UserJoinedRoomNotification.fromByteArray(message)));
                }
                case USER_LEFT_ROOM -> {
                    raise(
                            ServerMessageEvent.ROOM_LEFT,
                            new RoomLeftEvent(UserLeftRoomNotification.fromByteArray(message)));
                }
                case ROOM_TICKERS -> {
                    raise(
                            ServerMessageEvent.ROOM_TICKER_LIST_RECEIVED,
                            new RoomTickerListReceivedEvent(RoomTickerListNotification.fromByteArray(message)));
                }
                case ROOM_TICKER_ADD -> {
                    RoomTickerAddedNotification added = RoomTickerAddedNotification.fromByteArray(message);
                    raise(
                            ServerMessageEvent.ROOM_TICKER_ADDED,
                            new RoomTickerAddedEvent(added.getRoomName(), added.getTicker()));
                }
                case ROOM_TICKER_REMOVE -> {
                    RoomTickerRemovedNotification removed = RoomTickerRemovedNotification.fromByteArray(message);
                    raise(
                            ServerMessageEvent.ROOM_TICKER_REMOVED,
                            new RoomTickerRemovedEvent(removed.getRoomName(), removed.getUsername()));
                }
                case PRIVATE_ROOM_ADD_USER -> {
                    PrivateRoomAddUser response = PrivateRoomAddUser.fromByteArray(message);
                    waiter.complete(new WaitKey(code, response.getRoomName(), response.getUsername()));
                }
                case PRIVATE_ROOM_REMOVE_USER -> {
                    PrivateRoomRemoveUser response = PrivateRoomRemoveUser.fromByteArray(message);
                    waiter.complete(new WaitKey(code, response.getRoomName(), response.getUsername()));
                }
                case PRIVATE_ROOM_ADD_OPERATOR -> {
                    PrivateRoomAddOperator response = PrivateRoomAddOperator.fromByteArray(message);
                    waiter.complete(new WaitKey(code, response.getRoomName(), response.getUsername()));
                }
                case PRIVATE_ROOM_REMOVE_OPERATOR -> {
                    PrivateRoomRemoveOperator response = PrivateRoomRemoveOperator.fromByteArray(message);
                    waiter.complete(new WaitKey(code, response.getRoomName(), response.getUsername()));
                }
                case KICKED_FROM_SERVER -> {
                    raise(ServerMessageEvent.KICKED_FROM_SERVER, null);
                }
                case FILE_SEARCH -> handleSearchRequest(message);
                case EMBEDDED_MESSAGE -> {
                    distributedMessages.get().handleEmbeddedMessage(message);
                }
                default -> {
                    diagnostic.debug("Unhandled server message: " + code + "; " + message.length + " bytes");
                }
            }
        } catch (Throwable failure) {
            Throwable cause = unwrap(failure);
            diagnostic.warning("Error handling server message: " + code + "; " + failureMessage(cause), cause);
        }
    }

    @Override
    public void handleMessageWritten(MessageConnection sender, MessageEvent eventData) {
        MessageCode.Server code = new MessageReader<>(eventData.getMessage(), MessageCode.Server.class).readCode();
        diagnostic.debug("Server message sent: " + code);
    }

    private void handlePrivilegeNotification(byte[] message) {
        PrivilegeNotification notification = PrivilegeNotification.fromByteArray(message);
        raise(
                ServerMessageEvent.PRIVILEGE_NOTIFICATION_RECEIVED,
                new PrivilegeNotificationReceivedEvent(notification.getUsername(), notification.getId()));
        if (!options.get().isAutoAcknowledgePrivilegeNotifications()) {
            return;
        }
        // Off the server read loop: acknowledging writes back to the server.
        NetworkExecutor.dispatch(
                () -> server.acknowledgePrivilegeNotification(notification.getId(), CancellationSignal.none()),
                failure -> warnServerMessage(MessageCode.Server.NOTIFY_PRIVILEGES, failure));
    }

    private void handlePrivateMessage(byte[] message) {
        PrivateMessageNotification notification = PrivateMessageNotification.fromByteArray(message);
        raise(ServerMessageEvent.PRIVATE_MESSAGE_RECEIVED, new PrivateMessageReceivedEvent(notification));
        if (!options.get().isAutoAcknowledgePrivateMessages()) {
            return;
        }
        // Off the server read loop: acknowledging writes back to the server.
        NetworkExecutor.dispatch(
                () -> server.acknowledgePrivateMessage(notification.getId(), CancellationSignal.none()),
                failure -> warnServerMessage(MessageCode.Server.PRIVATE_MESSAGE, failure));
    }

    private void handleNetInfo(byte[] message) {
        NetInfoNotification notification = NetInfoNotification.fromByteArray(message);
        List<PeerEndpoint> parents = notification.getParents().stream()
                .map(parent ->
                        new PeerEndpoint(parent.username(), new InetSocketAddress(parent.ipAddress(), parent.port())))
                .toList();
        // Off the server read loop: this negotiates with every candidate before
        // it returns, and the server has more to say meanwhile.
        NetworkExecutor.dispatch(
                () -> distributed.get().addParentConnection(parents),
                failure -> diagnostic.debug("Error handling NetInfo message: " + failureMessage(unwrap(failure))));
    }

    private void handleCannotConnect(byte[] message) {
        CannotConnect cannotConnect = CannotConnect.fromByteArray(message);
        diagnostic.debug("Received CannotConnect message for token "
                + cannotConnect.getToken()
                + (cannotConnect.getUsername() == null
                                || cannotConnect.getUsername().isEmpty()
                        ? ""
                        : " from user " + cannotConnect.getUsername()));
        searchResponses.get().tryDiscard(cannotConnect.getToken());
        if (cannotConnect.getUsername() != null && !cannotConnect.getUsername().isEmpty()) {
            raise(ServerMessageEvent.USER_CANNOT_CONNECT, new UserCannotConnectEvent(cannotConnect));
        }
    }

    private void handleConnectToPeer(byte[] message) {
        ConnectToPeerResponse response = ConnectToPeerResponse.fromByteArray(message);
        try {
            switch (response.getType()) {
                case Constants.ConnectionType.TRANSFER -> handleTransferConnection(response);
                case Constants.ConnectionType.PEER -> {
                    diagnostic.debug("Received message ConnectToPeer request from "
                            + response.getUsername() + " ("
                            + response.getIpEndpoint() + ")");
                    // Off the server read loop: establishing this connects to
                    // a peer and writes to it, and the server has more to say
                    // meanwhile.
                    NetworkExecutor.dispatch(
                            () -> peers.get().getOrAddMessageConnection(response),
                            failure -> debugConnectToPeer(response, failure));
                }
                case Constants.ConnectionType.DISTRIBUTED -> {
                    diagnostic.debug("Received distributed ConnectToPeer request from "
                            + response.getUsername() + " ("
                            + response.getIpEndpoint() + ")");
                    // Off the server read loop, as above.
                    NetworkExecutor.dispatch(
                            () -> distributed.get().getOrAddChildConnection(response),
                            failure -> debugConnectToPeer(response, failure));
                }
                default ->
                    throw new MessageException("Unknown Connect To Peer connection type '" + response.getType() + "'");
            }
        } catch (Throwable failure) {
            debugConnectToPeer(response, failure);
        }
    }

    private void handleTransferConnection(ConnectToPeerResponse response) {
        diagnostic.debug("Received transfer ConnectToPeer request from "
                + response.getUsername() + " ("
                + response.getIpEndpoint() + ") for remote token "
                + response.getToken());
        boolean expected = !downloads.get().isEmpty()
                && downloads.get().values().stream()
                        .anyMatch(transfer -> Objects.equals(transfer.getUsername(), response.getUsername()));
        if (!expected) {
            throw new SoulseekClientException("Unexpected transfer request from "
                    + response.getUsername() + " ("
                    + response.getIpEndpoint() + "); Ignored");
        }
        // Off the server read loop, as above.
        NetworkExecutor.dispatch(
                () -> correlateTransferConnection(response, peers.get().getTransferConnection(response)),
                failure -> debugConnectToPeer(response, failure));
    }

    private void correlateTransferConnection(ConnectToPeerResponse response, TransferConnectionResult result) {
        TransferInternal download = downloads.get().values().stream()
                .filter(transfer -> Objects.equals(transfer.getRemoteToken(), result.remoteToken())
                        && Objects.equals(transfer.getUsername(), response.getUsername()))
                .findFirst()
                .orElse(null);
        if (download == null) {
            diagnostic.debug("Transfer ConnectToPeer request from "
                    + response.getUsername() + " ("
                    + response.getIpEndpoint() + ") for remote token "
                    + response.getToken()
                    + " does not match any waiting downloads, discarding.");
            result.connection().disconnect("Unknown transfer");
            return;
        }
        Connection connection = result.connection();
        diagnostic.debug("Solicited inbound transfer connection to "
                + download.getUsername() + " ("
                + connection.getIpEndpoint() + ") for token "
                + download.getToken() + " (remote: "
                + download.getRemoteToken() + ") established. (id: "
                + connection.getId() + ")");
        waiter.complete(
                new WaitKey(
                        Constants.WaitKey.INDIRECT_TRANSFER,
                        download.getUsername(),
                        download.getFilename(),
                        download.getRemoteToken()),
                connection);
    }

    private void handleSearchRequest(byte[] message) {
        ServerSearchRequest request = ServerSearchRequest.fromByteArray(message);
        if (Objects.equals(request.getUsername(), server.username())) {
            boolean deliberate = searches.get().values().stream()
                    .anyMatch(search -> deliberatelySearchesSelf(search, request.getToken()));
            if (!deliberate) {
                return;
            }
        }
        // Off the server read loop: answering asks the share catalog, connects
        // to the searcher and writes to them.
        NetworkExecutor.dispatch(
                () -> searchResponses.get().tryRespond(request.getUsername(), request.getToken(), request.getQuery()),
                failure -> warnServerMessage(MessageCode.Server.FILE_SEARCH, failure));
    }

    private boolean deliberatelySearchesSelf(SearchInternal search, int token) {
        if (search.getToken() != token || search.getScope().getType() != SearchScopeType.USER) {
            return false;
        }
        for (String subject : search.getScope().getSubjects()) {
            if (subject.equalsIgnoreCase(server.username())) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private <T> void raise(ServerMessageEvent event, T eventData) {
        List<ServerMessageHandlerEventListener<?>> snapshot = new ArrayList<>(listeners.get(event));
        for (ServerMessageHandlerEventListener<?> listener : snapshot) {
            ((ServerMessageHandlerEventListener<T>) listener).handle(this, eventData);
        }
    }

    private void raiseDiagnostic(DiagnosticEvent eventData) {
        diagnosticListeners.forEach(listener -> listener.handle(this, eventData));
    }

    private static int integer(byte[] message) {
        return IntegerResponse.fromByteArray(message, MessageCode.Server.class);
    }

    private static String string(byte[] message) {
        return StringResponse.fromByteArray(message, MessageCode.Server.class);
    }

    private void warnServerMessage(MessageCode.Server code, Throwable failure) {
        Throwable cause = unwrap(failure);
        diagnostic.warning("Error handling server message: " + code + "; " + failureMessage(cause), cause);
    }

    private void debugConnectToPeer(ConnectToPeerResponse response, Throwable failure) {
        diagnostic.debug("Error handling ConnectToPeer response from "
                + response.getUsername() + " ("
                + response.getIpEndpoint() + "): "
                + failureMessage(unwrap(failure)));
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String failureMessage(Throwable failure) {
        return failure.getMessage() == null ? "" : failure.getMessage();
    }
}
