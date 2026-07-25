// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static dev.slsk.ClientSupport.mapClientFailure;
import static dev.slsk.ClientSupport.requireNonEmpty;
import static dev.slsk.ClientSupport.requireText;
import static dev.slsk.ClientSupport.unwrap;

import dev.slsk.common.WaitKey;
import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.RoomJoinForbiddenException;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.messages.JoinRoomRequest;
import dev.slsk.messaging.messages.LeaveRoomRequest;
import dev.slsk.messaging.messages.PrivateRoomAddOperator;
import dev.slsk.messaging.messages.PrivateRoomAddUser;
import dev.slsk.messaging.messages.PrivateRoomDropMembershipCommand;
import dev.slsk.messaging.messages.PrivateRoomDropOwnershipCommand;
import dev.slsk.messaging.messages.PrivateRoomRemoveOperator;
import dev.slsk.messaging.messages.PrivateRoomRemoveUser;
import dev.slsk.messaging.messages.RoomListRequest;
import dev.slsk.messaging.messages.RoomMessageCommand;
import dev.slsk.messaging.messages.SetRoomTickerCommand;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

/**
 * Chat rooms: joining, leaving, messaging, tickers and private-room membership.
 *
 * <p>One of the pieces {@code DefaultSoulseekClient} was split into. Room
 * operations are all the same shape — register a correlated wait, write a
 * command, translate the reply — and none of them share mutable state with the
 * rest of the client, which makes them the cleanest thing to lift out first.
 *
 * <p>Everything it needs from the client arrives through {@link ClientContext}.
 */
final class RoomRegistry {

    private final ClientContext context;

    RoomRegistry(ClientContext context) {
        this.context = Objects.requireNonNull(context, "context");
    }

    CompletableFuture<RoomData> joinRoom(String roomName) {
        return joinRoom(roomName, false, CancellationSignal.none());
    }

    CompletableFuture<RoomData> joinRoom(String roomName, boolean isPrivate) {
        return joinRoom(roomName, isPrivate, CancellationSignal.none());
    }

    CompletableFuture<RoomData> joinRoom(String roomName, CancellationSignal cancellationSignal) {
        return joinRoom(roomName, false, cancellationSignal);
    }

    CompletableFuture<RoomData> joinRoom(String roomName, boolean isPrivate, CancellationSignal cancellationSignal) {
        requireText(roomName, "roomName");
        context.requireLoggedIn("join a chat room");
        CancellationSignal token = context.defaultToken(cancellationSignal);
        CompletableFuture<RoomData> wait;
        try {
            wait = context.getWaiter()
                    .waitAsync(new WaitKey(MessageCode.Server.JOIN_ROOM, roomName), RoomData.class, null, token);
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
        CompletableFuture<RoomData> operation = context.writeToServer(new JoinRoomRequest(roomName, isPrivate), token)
                .thenCompose(ignored -> translatedWait);
        return mapClientFailure(
                operation,
                "Failed to join chat room " + roomName + ": ",
                RoomJoinForbiddenException.class,
                NoResponseException.class);
    }

    CompletableFuture<RoomList> getRoomList() {
        return getRoomList(CancellationSignal.none());
    }

    CompletableFuture<RoomList> getRoomList(CancellationSignal cancellationSignal) {
        context.requireLoggedIn("fetch the list of chat rooms");
        return context.executeCorrelatedRequest(
                new RoomListRequest(),
                new WaitKey(MessageCode.Server.ROOM_LIST),
                RoomList.class,
                cancellationSignal,
                "Failed to fetch the list of chat rooms from the server: ");
    }

    CompletableFuture<Void> addPrivateRoomMember(String roomName, String requestedUsername) {
        return addPrivateRoomMember(roomName, requestedUsername, CancellationSignal.none());
    }

    CompletableFuture<Void> addPrivateRoomMember(
            String roomName, String requestedUsername, CancellationSignal cancellationSignal) {
        requireText(roomName, "roomName");
        requireText(requestedUsername, "username");
        context.requireLoggedIn("add members to private rooms");
        return context.executeCorrelatedCommand(
                new PrivateRoomAddUser(roomName, requestedUsername),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_ADD_USER, roomName, requestedUsername),
                cancellationSignal,
                "Failed to add user " + requestedUsername + " as member of private room " + roomName + ": ");
    }

    CompletableFuture<Void> addPrivateRoomModerator(String roomName, String requestedUsername) {
        return addPrivateRoomModerator(roomName, requestedUsername, CancellationSignal.none());
    }

    CompletableFuture<Void> addPrivateRoomModerator(
            String roomName, String requestedUsername, CancellationSignal cancellationSignal) {
        requireText(roomName, "roomName");
        requireText(requestedUsername, "username");
        context.requireLoggedIn("add moderators to private rooms");
        return context.executeCorrelatedCommand(
                new PrivateRoomAddOperator(roomName, requestedUsername),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_ADD_OPERATOR, roomName, requestedUsername),
                cancellationSignal,
                "Failed to add user " + requestedUsername + " as moderator of private room " + roomName + ": ");
    }

    CompletableFuture<Void> dropPrivateRoomMembership(String roomName) {
        return dropPrivateRoomMembership(roomName, CancellationSignal.none());
    }

    CompletableFuture<Void> dropPrivateRoomMembership(String roomName, CancellationSignal cancellationSignal) {
        requireText(roomName, "roomName");
        context.requireLoggedIn("drop private room membership");
        return context.executeCorrelatedCommand(
                new PrivateRoomDropMembershipCommand(roomName),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVED, roomName),
                cancellationSignal,
                "Failed to drop membership of private room " + roomName + ": ");
    }

    CompletableFuture<Void> dropPrivateRoomOwnership(String roomName) {
        return dropPrivateRoomOwnership(roomName, CancellationSignal.none());
    }

    CompletableFuture<Void> dropPrivateRoomOwnership(String roomName, CancellationSignal cancellationSignal) {
        requireText(roomName, "roomName");
        context.requireLoggedIn("drop private room ownership");
        return context.executeCorrelatedCommand(
                new PrivateRoomDropOwnershipCommand(roomName),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVED, roomName),
                cancellationSignal,
                "Failed to drop ownership of private room " + roomName + ": ");
    }

    CompletableFuture<Void> leaveRoom(String roomName) {
        return leaveRoom(roomName, CancellationSignal.none());
    }

    CompletableFuture<Void> leaveRoom(String roomName, CancellationSignal cancellationSignal) {
        requireText(roomName, "roomName");
        context.requireLoggedIn("leave a chat room");
        CancellationSignal token = context.defaultToken(cancellationSignal);
        CompletableFuture<Void> wait;
        try {
            wait = context.getWaiter().waitAsync(new WaitKey(MessageCode.Server.LEAVE_ROOM, roomName), null, token);
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
                context.writeToServer(new LeaveRoomRequest(roomName), token).thenCompose(ignored -> translatedWait);
        return mapClientFailure(operation, "Failed to leave chat room " + roomName + ": ", NoResponseException.class);
    }

    CompletableFuture<Void> removePrivateRoomMember(String roomName, String requestedUsername) {
        return removePrivateRoomMember(roomName, requestedUsername, CancellationSignal.none());
    }

    CompletableFuture<Void> removePrivateRoomMember(
            String roomName, String requestedUsername, CancellationSignal cancellationSignal) {
        requireText(roomName, "roomName");
        requireText(requestedUsername, "username");
        context.requireLoggedIn("remove users from private rooms");
        return context.executeCorrelatedCommand(
                new PrivateRoomRemoveUser(roomName, requestedUsername),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVE_USER, roomName, requestedUsername),
                cancellationSignal,
                "Failed to remove user " + requestedUsername + " as member of private room " + roomName + ": ");
    }

    CompletableFuture<Void> removePrivateRoomModerator(String roomName, String requestedUsername) {
        return removePrivateRoomModerator(roomName, requestedUsername, CancellationSignal.none());
    }

    CompletableFuture<Void> removePrivateRoomModerator(
            String roomName, String requestedUsername, CancellationSignal cancellationSignal) {
        requireText(roomName, "roomName");
        requireText(requestedUsername, "username");
        context.requireLoggedIn("remove moderators from private rooms");
        return context.executeCorrelatedCommand(
                new PrivateRoomRemoveOperator(roomName, requestedUsername),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVE_OPERATOR, roomName, requestedUsername),
                cancellationSignal,
                "Failed to remove user " + requestedUsername + " as moderator of private room " + roomName + ": ");
    }

    CompletableFuture<Void> sendRoomMessage(String roomName, String message) {
        return sendRoomMessage(roomName, message, CancellationSignal.none());
    }

    CompletableFuture<Void> sendRoomMessage(String roomName, String message, CancellationSignal cancellationSignal) {
        requireText(roomName, "roomName");
        requireNonEmpty(message, "message");
        context.requireLoggedIn("send a chat room message");
        return mapClientFailure(
                context.writeToServer(
                        new RoomMessageCommand(roomName, message), context.defaultToken(cancellationSignal)),
                "Failed to send message to room " + roomName + ": ");
    }

    CompletableFuture<Void> setRoomTicker(String roomName, String message) {
        return setRoomTicker(roomName, message, CancellationSignal.none());
    }

    CompletableFuture<Void> setRoomTicker(String roomName, String message, CancellationSignal cancellationSignal) {
        requireText(roomName, "roomName");
        requireNonEmpty(message, "message");
        context.requireLoggedIn("set chat room tickers");
        return mapClientFailure(
                context.writeToServer(
                        new SetRoomTickerCommand(roomName, message), context.defaultToken(cancellationSignal)),
                "Failed to set chat room ticker in room " + roomName + ": ");
    }
}
