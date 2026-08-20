// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.room.RoomTickerMessage;

/** Notification that a ticker was added to a chat room. */
public final class RoomTickerAddedNotification implements IncomingMessage {

    private final String roomName;
    private final RoomTickerMessage ticker;

    /** Creates a ticker-added notification. */
    public RoomTickerAddedNotification(String roomName, RoomTickerMessage ticker) {
        this.roomName = roomName;
        this.ticker = ticker;
    }

    public String getRoomName() {
        return roomName;
    }

    public RoomTickerMessage getTicker() {
        return ticker;
    }

    /** Parses a ticker-added notification. */
    public static RoomTickerAddedNotification fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader = ServerMessageParser.reader(
                bytes, MessageCode.Server.ROOM_TICKER_ADD, "RoomTickerAddedNotification", false);
        String roomName = reader.readString();
        return new RoomTickerAddedNotification(
                roomName, new RoomTickerMessage(reader.readString(), reader.readString()));
    }
}
