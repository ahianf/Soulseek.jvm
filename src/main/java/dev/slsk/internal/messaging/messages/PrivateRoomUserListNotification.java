// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.room.RoomInfoMessage;

/** Parses the users in a private chat room. */
public final class PrivateRoomUserListNotification implements IncomingMessage {

    private PrivateRoomUserListNotification() {}

    /** Parses the private-room user list. */
    public static RoomInfoMessage fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader = ServerMessageParser.reader(
                bytes, MessageCode.Server.PRIVATE_ROOM_USERS, "PrivateRoomUserListNotification", false);
        return PrivateRoomOwnedListNotification.readRoom(reader);
    }
}
