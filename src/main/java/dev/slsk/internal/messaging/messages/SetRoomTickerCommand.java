// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;

/** Sets a chat-room ticker. */
public final class SetRoomTickerCommand implements OutgoingMessage {
    private final String message;
    private final String roomName;

    public SetRoomTickerCommand(String roomName, String message) {
        this.roomName = roomName;
        this.message = message;
    }

    public String getRoomName() {
        return roomName;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.SET_ROOM_TICKER)
                .writeString(roomName)
                .writeString(message)
                .build();
    }
}
