// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.room.RoomInfoMessage;
import java.util.ArrayList;
import java.util.List;

/** Parses a private room and the users over whom this user has rights. */
public final class PrivateRoomOwnedListNotification implements IncomingMessage {

    private PrivateRoomOwnedListNotification() {}

    /** Parses the private-room ownership information. */
    public static RoomInfoMessage fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader = ServerMessageParser.reader(
                bytes, MessageCode.Server.PRIVATE_ROOM_OWNED, "PrivateRoomOwnedListNotification", false);
        return readRoom(reader);
    }

    static RoomInfoMessage readRoom(MessageReader<MessageCode.Server> reader) {
        String roomName = reader.readString();
        int count = reader.readInteger();
        List<String> users = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            users.add(reader.readString());
        }
        return new RoomInfoMessage(roomName, users);
    }
}
