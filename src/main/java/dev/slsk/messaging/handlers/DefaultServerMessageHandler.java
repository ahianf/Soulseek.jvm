// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.handlers;

import dev.slsk.CancellationToken;
import dev.slsk.RoomData;
import dev.slsk.RoomInfo;
import dev.slsk.RoomList;
import dev.slsk.SearchScopeType;
import dev.slsk.ServerInfo;
import dev.slsk.UserStatistics;
import dev.slsk.UserStatus;
import dev.slsk.common.Constants;
import dev.slsk.common.WaitKey;
import dev.slsk.diagnostics.DiagnosticEventArgs;
import dev.slsk.diagnostics.DiagnosticEventListener;
import dev.slsk.diagnostics.DiagnosticFactory;
import dev.slsk.diagnostics.IDiagnosticFactory;
import dev.slsk.eventargs.PrivateMessageReceivedEventArgs;
import dev.slsk.eventargs.PrivilegeNotificationReceivedEventArgs;
import dev.slsk.eventargs.PublicChatMessageReceivedEventArgs;
import dev.slsk.eventargs.RoomJoinedEventArgs;
import dev.slsk.eventargs.RoomLeftEventArgs;
import dev.slsk.eventargs.RoomMessageReceivedEventArgs;
import dev.slsk.eventargs.RoomTickerAddedEventArgs;
import dev.slsk.eventargs.RoomTickerListReceivedEventArgs;
import dev.slsk.eventargs.RoomTickerRemovedEventArgs;
import dev.slsk.eventargs.UserCannotConnectEventArgs;
import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.RoomJoinForbiddenException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;
import dev.slsk.messaging.messages.CannotConnect;
import dev.slsk.messaging.messages.CannotJoinRoomNotification;
import dev.slsk.messaging.messages.ConnectToPeerResponse;
import dev.slsk.messaging.messages.ExcludedSearchPhrasesNotification;
import dev.slsk.messaging.messages.GlobalMessageNotification;
import dev.slsk.messaging.messages.IntegerResponse;
import dev.slsk.messaging.messages.JoinRoomResponse;
import dev.slsk.messaging.messages.LeaveRoomResponse;
import dev.slsk.messaging.messages.LoginResponse;
import dev.slsk.messaging.messages.NetInfoNotification;
import dev.slsk.messaging.messages.NewPassword;
import dev.slsk.messaging.messages.PrivateMessageNotification;
import dev.slsk.messaging.messages.PrivateRoomAddOperator;
import dev.slsk.messaging.messages.PrivateRoomAddUser;
import dev.slsk.messaging.messages.PrivateRoomOwnedListNotification;
import dev.slsk.messaging.messages.PrivateRoomRemoveOperator;
import dev.slsk.messaging.messages.PrivateRoomRemoveUser;
import dev.slsk.messaging.messages.PrivateRoomToggle;
import dev.slsk.messaging.messages.PrivateRoomUserListNotification;
import dev.slsk.messaging.messages.PrivilegeNotification;
import dev.slsk.messaging.messages.PrivilegedUserListNotification;
import dev.slsk.messaging.messages.PrivilegedUserNotification;
import dev.slsk.messaging.messages.PublicChatMessageNotification;
import dev.slsk.messaging.messages.RoomListResponseFactory;
import dev.slsk.messaging.messages.RoomMessageNotification;
import dev.slsk.messaging.messages.RoomTickerAddedNotification;
import dev.slsk.messaging.messages.RoomTickerListNotification;
import dev.slsk.messaging.messages.RoomTickerRemovedNotification;
import dev.slsk.messaging.messages.ServerSearchRequest;
import dev.slsk.messaging.messages.StringResponse;
import dev.slsk.messaging.messages.UserAddressResponse;
import dev.slsk.messaging.messages.UserJoinedRoomNotification;
import dev.slsk.messaging.messages.UserLeftRoomNotification;
import dev.slsk.messaging.messages.UserPrivilegeResponse;
import dev.slsk.messaging.messages.UserStatisticsResponseFactory;
import dev.slsk.messaging.messages.UserStatusResponseFactory;
import dev.slsk.messaging.messages.WatchUserResponse;
import dev.slsk.network.MessageConnection;
import dev.slsk.network.MessageEventArgs;
import dev.slsk.network.PeerEndpoint;
import dev.slsk.network.TransferConnectionResult;
import dev.slsk.network.tcp.Connection;
import dev.slsk.search.SearchInternal;
import dev.slsk.transfer.TransferInternal;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;

/** Handles incoming messages from the server connection. */
public final class DefaultServerMessageHandler implements ServerMessageHandler {
    private final ServerMessageHandlerClient client;
    private final IDiagnosticFactory diagnostic;
    private final CopyOnWriteArrayList<DiagnosticEventListener> diagnosticListeners = new CopyOnWriteArrayList<>();
    private final Map<ServerMessageEvent, CopyOnWriteArrayList<ServerMessageHandlerEventListener<?>>> listeners =
            new EnumMap<>(ServerMessageEvent.class);

    /** Creates a handler with its default diagnostic factory. */
    public DefaultServerMessageHandler(ServerMessageHandlerClient client) {
        this(client, null);
    }

    /** Creates a handler. */
    public DefaultServerMessageHandler(ServerMessageHandlerClient client, IDiagnosticFactory diagnosticFactory) {
        this.client = Objects.requireNonNull(client, "client");
        diagnostic = diagnosticFactory == null
                ? new DiagnosticFactory(client.getOptions().getMinimumDiagnosticLevel(), this::raiseDiagnostic)
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
    public void handleMessageRead(MessageConnection sender, MessageEventArgs eventArgs) {
        handleMessageRead(sender, eventArgs.getMessage());
    }

    @Override
    public void handleMessageRead(MessageConnection sender, byte[] message) {
        handleMessageReadAsync(sender, message);
    }

    CompletableFuture<Void> handleMessageReadAsync(MessageConnection sender, byte[] message) {
        MessageCode.Server code = new MessageReader<>(message, MessageCode.Server.class).readCode();
        if (code != MessageCode.Server.EMBEDDED_MESSAGE) {
            diagnostic.debug("Server message received: " + code);
        }

        CompletableFuture<Void> operation;
        try {
            operation = switch (code) {
                case PARENT_MIN_SPEED -> {
                    raise(ServerMessageEvent.SERVER_INFO_RECEIVED, new ServerInfo(integer(message)));
                    yield completed();
                }
                case PARENT_SPEED_RATIO -> {
                    raise(ServerMessageEvent.SERVER_INFO_RECEIVED, new ServerInfo(null, integer(message)));
                    yield completed();
                }
                case WISHLIST_INTERVAL -> {
                    raise(ServerMessageEvent.SERVER_INFO_RECEIVED, new ServerInfo(null, null, integer(message)));
                    yield completed();
                }
                case CHECK_PRIVILEGES -> {
                    client.getWaiter().complete(new WaitKey(code), integer(message));
                    yield completed();
                }
                case PRIVATE_ROOM_ADDED -> {
                    raise(ServerMessageEvent.PRIVATE_ROOM_MEMBERSHIP_ADDED, string(message));
                    yield completed();
                }
                case PRIVATE_ROOM_REMOVED -> {
                    String room = string(message);
                    client.getWaiter().complete(new WaitKey(code, room));
                    raise(ServerMessageEvent.PRIVATE_ROOM_MEMBERSHIP_REMOVED, room);
                    yield completed();
                }
                case PRIVATE_ROOM_OPERATOR_ADDED -> {
                    raise(ServerMessageEvent.PRIVATE_ROOM_MODERATION_ADDED, string(message));
                    yield completed();
                }
                case PRIVATE_ROOM_OPERATOR_REMOVED -> {
                    String room = string(message);
                    client.getWaiter().complete(new WaitKey(code, room));
                    raise(ServerMessageEvent.PRIVATE_ROOM_MODERATION_REMOVED, room);
                    yield completed();
                }
                case NEW_PASSWORD -> {
                    client.getWaiter()
                            .complete(
                                    new WaitKey(code),
                                    NewPassword.fromByteArray(message).getPassword());
                    yield completed();
                }
                case PRIVATE_ROOM_TOGGLE -> {
                    client.getWaiter()
                            .complete(
                                    new WaitKey(code),
                                    PrivateRoomToggle.fromByteArray(message).isAcceptInvitations());
                    yield completed();
                }
                case EXCLUDED_SEARCH_PHRASES -> {
                    raise(
                            ServerMessageEvent.EXCLUDED_SEARCH_PHRASES_RECEIVED,
                            ExcludedSearchPhrasesNotification.fromByteArray(message));
                    yield completed();
                }
                case GLOBAL_ADMIN_MESSAGE -> {
                    raise(ServerMessageEvent.GLOBAL_MESSAGE_RECEIVED, GlobalMessageNotification.fromByteArray(message));
                    yield completed();
                }
                case PING -> {
                    client.getWaiter().complete(new WaitKey(code));
                    yield completed();
                }
                case LOGIN -> {
                    client.getWaiter().complete(new WaitKey(code), LoginResponse.fromByteArray(message));
                    yield completed();
                }
                case ROOM_LIST -> {
                    RoomList rooms = RoomListResponseFactory.fromByteArray(message);
                    client.getWaiter().complete(new WaitKey(code), rooms);
                    raise(ServerMessageEvent.ROOM_LIST_RECEIVED, rooms);
                    yield completed();
                }
                case PRIVATE_ROOM_OWNED -> {
                    RoomInfo room = PrivateRoomOwnedListNotification.fromByteArray(message);
                    raise(ServerMessageEvent.PRIVATE_ROOM_MODERATED_USER_LIST_RECEIVED, room);
                    yield completed();
                }
                case PRIVATE_ROOM_USERS -> {
                    RoomInfo room = PrivateRoomUserListNotification.fromByteArray(message);
                    raise(ServerMessageEvent.PRIVATE_ROOM_USER_LIST_RECEIVED, room);
                    yield completed();
                }
                case PRIVILEGED_USERS -> {
                    raise(
                            ServerMessageEvent.PRIVILEGED_USER_LIST_RECEIVED,
                            PrivilegedUserListNotification.fromByteArray(message));
                    yield completed();
                }
                case ADD_PRIVILEGED_USER -> {
                    raise(
                            ServerMessageEvent.PRIVILEGE_NOTIFICATION_RECEIVED,
                            new PrivilegeNotificationReceivedEventArgs(
                                    PrivilegedUserNotification.fromByteArray(message)));
                    yield completed();
                }
                case NOTIFY_PRIVILEGES -> handlePrivilegeNotification(message);
                case USER_PRIVILEGES -> {
                    UserPrivilegeResponse response = UserPrivilegeResponse.fromByteArray(message);
                    client.getWaiter().complete(new WaitKey(code, response.getUsername()), response.isPrivileged());
                    yield completed();
                }
                case NET_INFO -> handleNetInfo(message);
                case DISTRIBUTED_RESET -> {
                    diagnostic.info("Distributed network reset received from the server");
                    raise(ServerMessageEvent.DISTRIBUTED_NETWORK_RESET, null);
                    client.getDistributedConnectionManager().removeAndDisposeAll();
                    client.getDistributedConnectionManager().resetStatus();
                    yield completed();
                }
                case CANNOT_CONNECT -> {
                    handleCannotConnect(message);
                    yield completed();
                }
                case CANNOT_JOIN_ROOM -> {
                    CannotJoinRoomNotification rejected = CannotJoinRoomNotification.fromByteArray(message);
                    client.getWaiter()
                            .fail(
                                    new WaitKey(MessageCode.Server.JOIN_ROOM, rejected.getRoomName()),
                                    new RoomJoinForbiddenException(
                                            "The server rejected the request to join room " + rejected.getRoomName()));
                    yield completed();
                }
                case CONNECT_TO_PEER -> handleConnectToPeer(message);
                case WATCH_USER -> {
                    WatchUserResponse response = WatchUserResponse.fromByteArray(message);
                    client.getWaiter().complete(new WaitKey(code, response.getUsername()), response);
                    yield completed();
                }
                case GET_STATUS -> {
                    UserStatus status = UserStatusResponseFactory.fromByteArray(message);
                    client.getWaiter().complete(new WaitKey(code, status.getUsername()), status);
                    raise(ServerMessageEvent.USER_STATUS_CHANGED, status);
                    yield completed();
                }
                case GET_USER_STATS -> {
                    UserStatistics statistics = UserStatisticsResponseFactory.fromByteArray(message);
                    client.getWaiter().complete(new WaitKey(code, statistics.getUsername()), statistics);
                    raise(ServerMessageEvent.USER_STATISTICS_CHANGED, statistics);
                    yield completed();
                }
                case PRIVATE_MESSAGE -> handlePrivateMessage(message);
                case GET_PEER_ADDRESS -> {
                    UserAddressResponse response = UserAddressResponse.fromByteArray(message);
                    client.getWaiter().complete(new WaitKey(code, response.getUsername()), response);
                    yield completed();
                }
                case JOIN_ROOM -> {
                    RoomData response = JoinRoomResponse.fromByteArray(message);
                    client.getWaiter().complete(new WaitKey(code, response.getName()), response);
                    yield completed();
                }
                case LEAVE_ROOM -> {
                    LeaveRoomResponse response = LeaveRoomResponse.fromByteArray(message);
                    client.getWaiter().complete(new WaitKey(code, response.getRoomName()));
                    raise(
                            ServerMessageEvent.ROOM_LEFT,
                            new RoomLeftEventArgs(response.getRoomName(), client.getUsername()));
                    yield completed();
                }
                case SAY_IN_CHAT_ROOM -> {
                    raise(
                            ServerMessageEvent.ROOM_MESSAGE_RECEIVED,
                            new RoomMessageReceivedEventArgs(RoomMessageNotification.fromByteArray(message)));
                    yield completed();
                }
                case PUBLIC_CHAT -> {
                    raise(
                            ServerMessageEvent.PUBLIC_CHAT_MESSAGE_RECEIVED,
                            new PublicChatMessageReceivedEventArgs(
                                    PublicChatMessageNotification.fromByteArray(message)));
                    yield completed();
                }
                case USER_JOINED_ROOM -> {
                    raise(
                            ServerMessageEvent.ROOM_JOINED,
                            new RoomJoinedEventArgs(UserJoinedRoomNotification.fromByteArray(message)));
                    yield completed();
                }
                case USER_LEFT_ROOM -> {
                    raise(
                            ServerMessageEvent.ROOM_LEFT,
                            new RoomLeftEventArgs(UserLeftRoomNotification.fromByteArray(message)));
                    yield completed();
                }
                case ROOM_TICKERS -> {
                    raise(
                            ServerMessageEvent.ROOM_TICKER_LIST_RECEIVED,
                            new RoomTickerListReceivedEventArgs(RoomTickerListNotification.fromByteArray(message)));
                    yield completed();
                }
                case ROOM_TICKER_ADD -> {
                    RoomTickerAddedNotification added = RoomTickerAddedNotification.fromByteArray(message);
                    raise(
                            ServerMessageEvent.ROOM_TICKER_ADDED,
                            new RoomTickerAddedEventArgs(added.getRoomName(), added.getTicker()));
                    yield completed();
                }
                case ROOM_TICKER_REMOVE -> {
                    RoomTickerRemovedNotification removed = RoomTickerRemovedNotification.fromByteArray(message);
                    raise(
                            ServerMessageEvent.ROOM_TICKER_REMOVED,
                            new RoomTickerRemovedEventArgs(removed.getRoomName(), removed.getUsername()));
                    yield completed();
                }
                case PRIVATE_ROOM_ADD_USER -> {
                    PrivateRoomAddUser response = PrivateRoomAddUser.fromByteArray(message);
                    client.getWaiter().complete(new WaitKey(code, response.getRoomName(), response.getUsername()));
                    yield completed();
                }
                case PRIVATE_ROOM_REMOVE_USER -> {
                    PrivateRoomRemoveUser response = PrivateRoomRemoveUser.fromByteArray(message);
                    client.getWaiter().complete(new WaitKey(code, response.getRoomName(), response.getUsername()));
                    yield completed();
                }
                case PRIVATE_ROOM_ADD_OPERATOR -> {
                    PrivateRoomAddOperator response = PrivateRoomAddOperator.fromByteArray(message);
                    client.getWaiter().complete(new WaitKey(code, response.getRoomName(), response.getUsername()));
                    yield completed();
                }
                case PRIVATE_ROOM_REMOVE_OPERATOR -> {
                    PrivateRoomRemoveOperator response = PrivateRoomRemoveOperator.fromByteArray(message);
                    client.getWaiter().complete(new WaitKey(code, response.getRoomName(), response.getUsername()));
                    yield completed();
                }
                case KICKED_FROM_SERVER -> {
                    raise(ServerMessageEvent.KICKED_FROM_SERVER, null);
                    yield completed();
                }
                case FILE_SEARCH -> handleSearchRequest(message);
                case EMBEDDED_MESSAGE -> {
                    client.getDistributedMessageHandler().handleEmbeddedMessage(message);
                    yield completed();
                }
                default -> {
                    diagnostic.debug("Unhandled server message: " + code + "; " + message.length + " bytes");
                    yield completed();
                }
            };
        } catch (Throwable failure) {
            operation = CompletableFuture.failedFuture(failure);
        }

        return operation.handle((ignored, failure) -> {
            if (failure != null) {
                Throwable cause = unwrap(failure);
                diagnostic.warning("Error handling server message: " + code + "; " + failureMessage(cause), cause);
            }
            return null;
        });
    }

    @Override
    public void handleMessageWritten(MessageConnection sender, MessageEventArgs eventArgs) {
        MessageCode.Server code = new MessageReader<>(eventArgs.getMessage(), MessageCode.Server.class).readCode();
        diagnostic.debug("Server message sent: " + code);
    }

    private CompletableFuture<Void> handlePrivilegeNotification(byte[] message) {
        PrivilegeNotification notification = PrivilegeNotification.fromByteArray(message);
        raise(
                ServerMessageEvent.PRIVILEGE_NOTIFICATION_RECEIVED,
                new PrivilegeNotificationReceivedEventArgs(notification.getUsername(), notification.getId()));
        if (!client.getOptions().isAutoAcknowledgePrivilegeNotifications()) {
            return completed();
        }
        return client.acknowledgePrivilegeNotificationAsync(notification.getId(), CancellationToken.none());
    }

    private CompletableFuture<Void> handlePrivateMessage(byte[] message) {
        PrivateMessageNotification notification = PrivateMessageNotification.fromByteArray(message);
        raise(ServerMessageEvent.PRIVATE_MESSAGE_RECEIVED, new PrivateMessageReceivedEventArgs(notification));
        if (!client.getOptions().isAutoAcknowledgePrivateMessages()) {
            return completed();
        }
        return client.acknowledgePrivateMessageAsync(notification.getId(), CancellationToken.none());
    }

    private CompletableFuture<Void> handleNetInfo(byte[] message) {
        NetInfoNotification notification = NetInfoNotification.fromByteArray(message);
        List<PeerEndpoint> parents = notification.getParents().stream()
                .map(parent ->
                        new PeerEndpoint(parent.username(), new InetSocketAddress(parent.ipAddress(), parent.port())))
                .toList();
        CompletableFuture<Void> add;
        try {
            add = client.getDistributedConnectionManager().addParentConnectionAsync(parents);
        } catch (Throwable failure) {
            add = CompletableFuture.failedFuture(failure);
        }
        return add.handle((ignored, failure) -> {
            if (failure != null) {
                Throwable cause = unwrap(failure);
                diagnostic.debug("Error handling NetInfo message: " + failureMessage(cause));
            }
            return null;
        });
    }

    private void handleCannotConnect(byte[] message) {
        CannotConnect cannotConnect = CannotConnect.fromByteArray(message);
        diagnostic.debug("Received CannotConnect message for token "
                + cannotConnect.getToken()
                + (cannotConnect.getUsername() == null
                                || cannotConnect.getUsername().isEmpty()
                        ? ""
                        : " from user " + cannotConnect.getUsername()));
        client.getSearchResponder().tryDiscard(cannotConnect.getToken());
        if (cannotConnect.getUsername() != null && !cannotConnect.getUsername().isEmpty()) {
            raise(ServerMessageEvent.USER_CANNOT_CONNECT, new UserCannotConnectEventArgs(cannotConnect));
        }
    }

    private CompletableFuture<Void> handleConnectToPeer(byte[] message) {
        ConnectToPeerResponse response = ConnectToPeerResponse.fromByteArray(message);
        CompletableFuture<Void> operation;
        try {
            operation = switch (response.getType()) {
                case Constants.ConnectionType.TRANSFER -> handleTransferConnection(response);
                case Constants.ConnectionType.PEER -> {
                    diagnostic.debug("Received message ConnectToPeer request from "
                            + response.getUsername() + " ("
                            + response.getIpEndPoint() + ")");
                    yield client.getPeerConnectionManager()
                            .getOrAddMessageConnectionAsync(response)
                            .thenApply(ignored -> null);
                }
                case Constants.ConnectionType.DISTRIBUTED -> {
                    diagnostic.debug("Received distributed ConnectToPeer request from "
                            + response.getUsername() + " ("
                            + response.getIpEndPoint() + ")");
                    yield client.getDistributedConnectionManager().getOrAddChildConnectionAsync(response);
                }
                default ->
                    CompletableFuture.failedFuture(new MessageException(
                            "Unknown Connect To Peer connection type '" + response.getType() + "'"));
            };
        } catch (Throwable failure) {
            operation = CompletableFuture.failedFuture(failure);
        }
        return operation.handle((ignored, failure) -> {
            if (failure != null) {
                Throwable cause = unwrap(failure);
                diagnostic.debug("Error handling ConnectToPeer response from "
                        + response.getUsername() + " ("
                        + response.getIpEndPoint() + "): "
                        + failureMessage(cause));
            }
            return null;
        });
    }

    private CompletableFuture<Void> handleTransferConnection(ConnectToPeerResponse response) {
        diagnostic.debug("Received transfer ConnectToPeer request from "
                + response.getUsername() + " ("
                + response.getIpEndPoint() + ") for remote token "
                + response.getToken());
        boolean expected = !client.getDownloadDictionary().isEmpty()
                && client.getDownloadDictionary().values().stream()
                        .anyMatch(transfer -> Objects.equals(transfer.getUsername(), response.getUsername()));
        if (!expected) {
            return CompletableFuture.failedFuture(new SoulseekClientException("Unexpected transfer request from "
                    + response.getUsername() + " ("
                    + response.getIpEndPoint() + "); Ignored"));
        }
        return client.getPeerConnectionManager()
                .getTransferConnectionAsync(response)
                .thenAccept(result -> correlateTransferConnection(response, result));
    }

    private void correlateTransferConnection(ConnectToPeerResponse response, TransferConnectionResult result) {
        TransferInternal download = client.getDownloadDictionary().values().stream()
                .filter(transfer -> Objects.equals(transfer.getRemoteToken(), result.remoteToken())
                        && Objects.equals(transfer.getUsername(), response.getUsername()))
                .findFirst()
                .orElse(null);
        if (download == null) {
            diagnostic.debug("Transfer ConnectToPeer request from "
                    + response.getUsername() + " ("
                    + response.getIpEndPoint() + ") for remote token "
                    + response.getToken()
                    + " does not match any waiting downloads, discarding.");
            result.connection().disconnect("Unknown transfer");
            return;
        }
        Connection connection = result.connection();
        diagnostic.debug("Solicited inbound transfer connection to "
                + download.getUsername() + " ("
                + connection.getIpEndPoint() + ") for token "
                + download.getToken() + " (remote: "
                + download.getRemoteToken() + ") established. (id: "
                + connection.getId() + ")");
        client.getWaiter()
                .complete(
                        new WaitKey(
                                Constants.WaitKey.INDIRECT_TRANSFER,
                                download.getUsername(),
                                download.getFilename(),
                                download.getRemoteToken()),
                        connection);
    }

    private CompletableFuture<Void> handleSearchRequest(byte[] message) {
        ServerSearchRequest request = ServerSearchRequest.fromByteArray(message);
        if (Objects.equals(request.getUsername(), client.getUsername())) {
            boolean deliberate = client.getSearches().values().stream()
                    .anyMatch(search -> deliberatelySearchesSelf(search, request.getToken()));
            if (!deliberate) {
                return completed();
            }
        }
        return client.getSearchResponder()
                .tryRespondAsync(request.getUsername(), request.getToken(), request.getQuery())
                .thenApply(ignored -> null);
    }

    private boolean deliberatelySearchesSelf(SearchInternal search, int token) {
        if (search.getToken() != token || search.getScope().getType() != SearchScopeType.USER) {
            return false;
        }
        for (String subject : search.getScope().getSubjects()) {
            if (subject.equalsIgnoreCase(client.getUsername())) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private <T> void raise(ServerMessageEvent event, T eventArgs) {
        List<ServerMessageHandlerEventListener<?>> snapshot = new ArrayList<>(listeners.get(event));
        for (ServerMessageHandlerEventListener<?> listener : snapshot) {
            ((ServerMessageHandlerEventListener<T>) listener).handle(this, eventArgs);
        }
    }

    private void raiseDiagnostic(DiagnosticEventArgs eventArgs) {
        diagnosticListeners.forEach(listener -> listener.handle(this, eventArgs));
    }

    private static int integer(byte[] message) {
        return IntegerResponse.fromByteArray(message, MessageCode.Server.class);
    }

    private static String string(byte[] message) {
        return StringResponse.fromByteArray(message, MessageCode.Server.class);
    }

    private static CompletableFuture<Void> completed() {
        return CompletableFuture.completedFuture(null);
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
