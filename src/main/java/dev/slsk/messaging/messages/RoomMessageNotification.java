// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** An incoming chat-room message. */
public final class RoomMessageNotification implements IncomingMessage {
    private final String message;
    private final String roomName;
    private final String username;

    /** Creates a room-message notification. */
    public RoomMessageNotification(String roomName, String username, String message) {
        this.roomName = roomName;
        this.username = username;
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public String getRoomName() {
        return roomName;
    }

    public String getUsername() {
        return username;
    }

    /** Parses a room-message notification. */
    public static RoomMessageNotification fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.SAY_IN_CHAT_ROOM, "RoomMessageNotification");
        return new RoomMessageNotification(reader.readString(), reader.readString(), reader.readString());
    }
}
