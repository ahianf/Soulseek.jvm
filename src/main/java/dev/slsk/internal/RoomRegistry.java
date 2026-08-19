// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.RoomJoinForbiddenException;
import dev.slsk.internal.common.CommonUtils;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.messages.JoinRoomRequest;
import dev.slsk.internal.messaging.messages.LeaveRoomRequest;
import dev.slsk.internal.messaging.messages.PrivateRoomAddOperator;
import dev.slsk.internal.messaging.messages.PrivateRoomAddUser;
import dev.slsk.internal.messaging.messages.PrivateRoomDropMembershipCommand;
import dev.slsk.internal.messaging.messages.PrivateRoomDropOwnershipCommand;
import dev.slsk.internal.messaging.messages.PrivateRoomRemoveOperator;
import dev.slsk.internal.messaging.messages.PrivateRoomRemoveUser;
import dev.slsk.internal.messaging.messages.RoomListRequest;
import dev.slsk.internal.messaging.messages.RoomMessageCommand;
import dev.slsk.internal.messaging.messages.SetRoomTickerCommand;
import dev.slsk.internal.room.RoomData;
import dev.slsk.internal.room.RoomList;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * Chat rooms: joining, leaving, messaging, tickers and private-room membership.
 *
 * <p>One of the pieces {@code SoulseekEngine} was split into. Room
 * operations are all the same shape — register a correlated wait, write a
 * command, translate the reply — and none of them share mutable state with the
 * rest of the client, which makes them the cleanest thing to lift out first.
 *
 * <p>What it needs of the client is the correlator and the server link, and
 * nothing else; the two are what a room operation is made of.
 */
final class RoomRegistry {

    private final Waiter waiter;
    private final ServerLink server;

    RoomRegistry(Waiter waiter, ServerLink server) {
        this.waiter = Objects.requireNonNull(waiter, "waiter");
        this.server = Objects.requireNonNull(server, "server");
    }

    RoomData joinRoom(String roomName) {
        return joinRoom(roomName, false, CancellationSignal.none());
    }

    RoomData joinRoom(String roomName, boolean isPrivate) {
        return joinRoom(roomName, isPrivate, CancellationSignal.none());
    }

    RoomData joinRoom(String roomName, CancellationSignal cancellationSignal) {
        return joinRoom(roomName, false, cancellationSignal);
    }

    RoomData joinRoom(String roomName, boolean isPrivate, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(roomName, "roomName");
        server.requireLoggedIn("join a chat room");
        CancellationSignal token = CommonUtils.token(cancellationSignal);
        try {
            // Registered before the write, because the server can answer
            // before the write call returns.
            Wait<RoomData> wait =
                    waiter.register(new WaitKey(MessageCode.Server.JOIN_ROOM, roomName), RoomData.class, null, token);
            server.write(new JoinRoomRequest(roomName, isPrivate), token);
            try {
                return wait.await();
            } catch (Throwable failure) {
                Throwable cause = Failures.unwrap(failure);
                if (cause instanceof TimeoutException) {
                    throw new NoResponseException("The server didn't respond to the request "
                            + "to join chat room " + roomName
                            + ". This probably indicates that the "
                            + "room is already joined.");
                }
                throw Failures.propagate(cause);
            }
        } catch (Throwable failure) {
            throw Failures.raise(
                    failure,
                    "Failed to join chat room " + roomName + ": ",
                    RoomJoinForbiddenException.class,
                    NoResponseException.class);
        }
    }

    RoomList getRoomList() {
        return getRoomList(CancellationSignal.none());
    }

    RoomList getRoomList(CancellationSignal cancellationSignal) {
        server.requireLoggedIn("fetch the list of chat rooms");
        return server.request(
                new RoomListRequest(),
                new WaitKey(MessageCode.Server.ROOM_LIST),
                RoomList.class,
                cancellationSignal,
                "Failed to fetch the list of chat rooms from the server: ");
    }

    void addPrivateRoomMember(String roomName, String requestedUsername) {
        addPrivateRoomMember(roomName, requestedUsername, CancellationSignal.none());
    }

    void addPrivateRoomMember(String roomName, String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(roomName, "roomName");
        CommonUtils.requireText(requestedUsername, "username");
        server.requireLoggedIn("add members to private rooms");
        server.command(
                new PrivateRoomAddUser(roomName, requestedUsername),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_ADD_USER, roomName, requestedUsername),
                cancellationSignal,
                "Failed to add user " + requestedUsername + " as member of private room " + roomName + ": ");
    }

    void addPrivateRoomModerator(String roomName, String requestedUsername) {
        addPrivateRoomModerator(roomName, requestedUsername, CancellationSignal.none());
    }

    void addPrivateRoomModerator(String roomName, String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(roomName, "roomName");
        CommonUtils.requireText(requestedUsername, "username");
        server.requireLoggedIn("add moderators to private rooms");
        server.command(
                new PrivateRoomAddOperator(roomName, requestedUsername),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_ADD_OPERATOR, roomName, requestedUsername),
                cancellationSignal,
                "Failed to add user " + requestedUsername + " as moderator of private room " + roomName + ": ");
    }

    void dropPrivateRoomMembership(String roomName) {
        dropPrivateRoomMembership(roomName, CancellationSignal.none());
    }

    void dropPrivateRoomMembership(String roomName, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(roomName, "roomName");
        server.requireLoggedIn("drop private room membership");
        server.command(
                new PrivateRoomDropMembershipCommand(roomName),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVED, roomName),
                cancellationSignal,
                "Failed to drop membership of private room " + roomName + ": ");
    }

    void dropPrivateRoomOwnership(String roomName) {
        dropPrivateRoomOwnership(roomName, CancellationSignal.none());
    }

    void dropPrivateRoomOwnership(String roomName, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(roomName, "roomName");
        server.requireLoggedIn("drop private room ownership");
        server.command(
                new PrivateRoomDropOwnershipCommand(roomName),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVED, roomName),
                cancellationSignal,
                "Failed to drop ownership of private room " + roomName + ": ");
    }

    void leaveRoom(String roomName) {
        leaveRoom(roomName, CancellationSignal.none());
    }

    void leaveRoom(String roomName, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(roomName, "roomName");
        server.requireLoggedIn("leave a chat room");
        CancellationSignal token = CommonUtils.token(cancellationSignal);
        try {
            Wait<Void> wait = waiter.register(new WaitKey(MessageCode.Server.LEAVE_ROOM, roomName), null, token);
            server.write(new LeaveRoomRequest(roomName), token);
            try {
                wait.await();
            } catch (Throwable failure) {
                Throwable cause = Failures.unwrap(failure);
                if (cause instanceof TimeoutException) {
                    throw new NoResponseException("The server didn't respond to the request "
                            + "to leave chat room " + roomName
                            + ".  This probably indicates that the "
                            + "room is not joined.");
                }
                throw Failures.propagate(cause);
            }
        } catch (Throwable failure) {
            throw Failures.raise(failure, "Failed to leave chat room " + roomName + ": ", NoResponseException.class);
        }
    }

    void removePrivateRoomMember(String roomName, String requestedUsername) {
        removePrivateRoomMember(roomName, requestedUsername, CancellationSignal.none());
    }

    void removePrivateRoomMember(String roomName, String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(roomName, "roomName");
        CommonUtils.requireText(requestedUsername, "username");
        server.requireLoggedIn("remove users from private rooms");
        server.command(
                new PrivateRoomRemoveUser(roomName, requestedUsername),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVE_USER, roomName, requestedUsername),
                cancellationSignal,
                "Failed to remove user " + requestedUsername + " as member of private room " + roomName + ": ");
    }

    void removePrivateRoomModerator(String roomName, String requestedUsername) {
        removePrivateRoomModerator(roomName, requestedUsername, CancellationSignal.none());
    }

    void removePrivateRoomModerator(String roomName, String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(roomName, "roomName");
        CommonUtils.requireText(requestedUsername, "username");
        server.requireLoggedIn("remove moderators from private rooms");
        server.command(
                new PrivateRoomRemoveOperator(roomName, requestedUsername),
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVE_OPERATOR, roomName, requestedUsername),
                cancellationSignal,
                "Failed to remove user " + requestedUsername + " as moderator of private room " + roomName + ": ");
    }

    void sendRoomMessage(String roomName, String message) {
        sendRoomMessage(roomName, message, CancellationSignal.none());
    }

    void sendRoomMessage(String roomName, String message, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(roomName, "roomName");
        CommonUtils.requireNonEmpty(message, "message");
        server.requireLoggedIn("send a chat room message");
        try {
            server.write(new RoomMessageCommand(roomName, message), CommonUtils.token(cancellationSignal));
        } catch (Throwable failure) {
            throw Failures.raise(failure, "Failed to send message to room " + roomName + ": ");
        }
    }

    void setRoomTicker(String roomName, String message) {
        setRoomTicker(roomName, message, CancellationSignal.none());
    }

    void setRoomTicker(String roomName, String message, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(roomName, "roomName");
        CommonUtils.requireNonEmpty(message, "message");
        server.requireLoggedIn("set chat room tickers");
        try {
            server.write(new SetRoomTickerCommand(roomName, message), CommonUtils.token(cancellationSignal));
        } catch (Throwable failure) {
            throw Failures.raise(failure, "Failed to set chat room ticker in room " + roomName + ": ");
        }
    }
}
