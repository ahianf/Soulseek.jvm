// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;

/** Sends a message to a chat room. */
public final class RoomMessageCommand implements OutgoingMessage {
    private final String message;
    private final String roomName;

    public RoomMessageCommand(String roomName, String message) {
        this.roomName = roomName;
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public String getRoomName() {
        return roomName;
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.SAY_IN_CHAT_ROOM)
                .writeString(roomName)
                .writeString(message)
                .build();
    }
}
