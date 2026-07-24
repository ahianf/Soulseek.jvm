// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** Notification that a ticker was removed from a chat room. */
public final class RoomTickerRemovedNotification implements IncomingMessage {

    private final String roomName;
    private final String username;

    /** Creates a ticker-removed notification. */
    public RoomTickerRemovedNotification(String roomName, String username) {
        this.roomName = roomName;
        this.username = username;
    }

    public String getRoomName() {
        return roomName;
    }

    public String getUsername() {
        return username;
    }

    /** Parses a ticker-removed notification. */
    public static RoomTickerRemovedNotification fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader = ServerMessageParser.reader(
                bytes, MessageCode.Server.ROOM_TICKER_REMOVE, "RoomTickerRemovedNotification", false);
        return new RoomTickerRemovedNotification(reader.readString(), reader.readString());
    }
}
