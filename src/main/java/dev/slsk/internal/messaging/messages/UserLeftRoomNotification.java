// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;

/** Notification that a user left a chat room. */
public final class UserLeftRoomNotification implements IncomingMessage {
    private final String roomName;
    private final String username;

    /** Creates a user-left notification. */
    public UserLeftRoomNotification(String roomName, String username) {
        this.roomName = roomName;
        this.username = username;
    }

    public String getRoomName() {
        return roomName;
    }

    public String getUsername() {
        return username;
    }

    /** Parses a user-left notification. */
    public static UserLeftRoomNotification fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.USER_LEFT_ROOM, "UserLeftRoomNotification");
        return new UserLeftRoomNotification(reader.readString(), reader.readString());
    }
}
