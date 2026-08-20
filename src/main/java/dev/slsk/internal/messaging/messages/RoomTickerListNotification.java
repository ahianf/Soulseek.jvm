// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.room.RoomTicker;
import java.util.ArrayList;
import java.util.List;

/** An incoming list of tickers for a chat room. */
public record RoomTickerListNotification(String roomName, int tickerCount, List<RoomTicker> tickers)
        implements IncomingMessage {

    public RoomTickerListNotification {
        tickers = List.copyOf(tickers);
    }

    public static RoomTickerListNotification fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.ROOM_TICKERS, "RoomTickerListNotification", false);
        String roomName = reader.readString();
        int tickerCount = reader.readInteger();
        List<RoomTicker> tickers = new ArrayList<>();
        for (int index = 0; index < tickerCount; index++) {
            tickers.add(new RoomTicker(reader.readString(), reader.readString()));
        }
        return new RoomTickerListNotification(roomName, tickerCount, tickers);
    }
}
