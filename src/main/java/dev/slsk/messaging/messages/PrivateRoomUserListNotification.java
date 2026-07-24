// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.RoomInfo;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** Parses the users in a private chat room. */
public final class PrivateRoomUserListNotification implements IIncomingMessage {

    private PrivateRoomUserListNotification() {}

    /** Parses the private-room user list. */
    public static RoomInfo fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader = ServerMessageParser.reader(
                bytes, MessageCode.Server.PRIVATE_ROOM_USERS, "PrivateRoomUserListNotification", false);
        return PrivateRoomOwnedListNotification.readRoom(reader);
    }
}
