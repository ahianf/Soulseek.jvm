// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;

/** Requests a search from all members of a room. */
public final class RoomSearchRequest implements OutgoingMessage {
    private final String roomName;
    private final String searchText;
    private final int token;

    public RoomSearchRequest(String roomName, String searchText, int token) {
        this.roomName = roomName;
        this.searchText = searchText;
        this.token = token;
    }

    public String getRoomName() {
        return roomName;
    }

    public String getSearchText() {
        return searchText;
    }

    public int getToken() {
        return token;
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.ROOM_SEARCH)
                .writeString(roomName)
                .writeInteger(token)
                .writeString(searchText)
                .build();
    }
}
