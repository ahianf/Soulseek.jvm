// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;

/** Reports an unsuccessful attempt to join a chat room. */
public final class CannotJoinRoomNotification implements IncomingMessage {

    private final String roomName;

    /** Creates a cannot-join notification. */
    public CannotJoinRoomNotification(String roomName) {
        this.roomName = roomName;
    }

    public String getRoomName() {
        return roomName;
    }

    /** Parses a cannot-join notification. */
    public static CannotJoinRoomNotification fromByteArray(byte[] bytes) {
        return new CannotJoinRoomNotification(
                ServerMessageParser.reader(bytes, MessageCode.Server.CANNOT_JOIN_ROOM, "CannotJoinRoomNotification")
                        .readString());
    }
}
