// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.RoomTicker;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** An incoming list of tickers for a chat room. */
public final class RoomTickerListNotification implements IncomingMessage {

    private final String roomName;
    private final int tickerCount;
    private final List<RoomTicker> tickers;

    /** Creates a ticker-list notification. */
    public RoomTickerListNotification(String roomName, int tickerCount, Iterable<? extends RoomTicker> tickers) {
        this.roomName = roomName;
        this.tickerCount = tickerCount;
        Objects.requireNonNull(tickers, "tickers");
        List<RoomTicker> copy = new ArrayList<>();
        tickers.forEach(copy::add);
        this.tickers = Collections.unmodifiableList(copy);
    }

    public String getRoomName() {
        return roomName;
    }

    public int getTickerCount() {
        return tickerCount;
    }

    public List<RoomTicker> getTickers() {
        return tickers;
    }

    /** Parses a ticker-list notification. */
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
