// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;

/** Joins a chat room. */
public final class JoinRoomRequest implements OutgoingMessage {
    private final boolean isPrivate;
    private final String roomName;

    public JoinRoomRequest(String roomName) {
        this(roomName, false);
    }

    public JoinRoomRequest(String roomName, boolean isPrivate) {
        this.roomName = roomName;
        this.isPrivate = isPrivate;
    }

    public String getRoomName() {
        return roomName;
    }

    public boolean isPrivate() {
        return isPrivate;
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.JOIN_ROOM)
                .writeString(roomName)
                .writeInteger(isPrivate ? 1 : 0)
                .build();
    }
}
