// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** An incoming public-chat message. */
public final class PublicChatMessageNotification implements IncomingMessage {

    private final String message;
    private final String roomName;
    private final String username;

    /** Creates a public-chat message notification. */
    public PublicChatMessageNotification(String roomName, String username, String message) {
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

    /** Parses a public-chat message notification. */
    public static PublicChatMessageNotification fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.PUBLIC_CHAT, "PublicChatMessageNotification");
        return new PublicChatMessageNotification(reader.readString(), reader.readString(), reader.readString());
    }
}
