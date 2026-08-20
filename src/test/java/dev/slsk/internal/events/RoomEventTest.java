// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.internal.room.RoomTicker;
import dev.slsk.internal.user.UserData;
import dev.slsk.internal.user.WireUserPresence;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RoomEventTest {
    @Test
    @DisplayName("RoomJoinedEvent instantiates with expected values")
    void joinedInstantiatesWithExpectedValues() {
        UserData userData = new UserData("alice", WireUserPresence.ONLINE, 1, 2, 3, 4, "CL");
        RoomJoinedEvent args = new RoomJoinedEvent("lobby", "alice", userData);

        assertEquals("lobby", args.roomName());
        assertEquals("alice", args.username());
        assertSame(userData, args.userData());
    }

    @Test
    @DisplayName("RoomLeftEvent instantiates with expected values")
    void leftInstantiatesWithExpectedValues() {
        RoomLeftEvent args = new RoomLeftEvent("lobby", "alice");

        assertEquals("lobby", args.roomName());
        assertEquals("alice", args.username());
    }

    @Test
    @DisplayName("RoomMessageReceivedEvent instantiates with expected values")
    void messageInstantiatesWithExpectedValues() {
        RoomMessageReceivedEvent args = new RoomMessageReceivedEvent("lobby", "alice", "hello");

        assertEquals("lobby", args.roomName());
        assertEquals("alice", args.username());
        assertEquals("hello", args.message());
    }

    @Test
    @DisplayName("Room event arguments preserve nullable references")
    void preservesNullableReferences() {
        RoomJoinedEvent joined = new RoomJoinedEvent(null, null, null);
        RoomMessageReceivedEvent message = new RoomMessageReceivedEvent(null, null, null);

        assertNull(joined.roomName());
        assertNull(joined.username());
        assertNull(joined.userData());
        assertNull(message.message());
    }

    @Test
    @DisplayName("RoomTickerAddedEvent instantiates with expected values")
    void tickerAddedInstantiatesWithExpectedValues() {
        RoomTicker ticker = new RoomTicker("alice", "hello");
        RoomTickerAddedEvent args = new RoomTickerAddedEvent("lobby", ticker);

        assertEquals("lobby", args.roomName());
        assertSame(ticker, args.ticker());
    }

    @Test
    @DisplayName("RoomTickerRemovedEvent instantiates with expected values")
    void tickerRemovedInstantiatesWithExpectedValues() {
        RoomTickerRemovedEvent args = new RoomTickerRemovedEvent("lobby", "alice");

        assertEquals("lobby", args.roomName());
        assertEquals("alice", args.username());
    }

    @Test
    @DisplayName("RoomTickerListReceivedEvent instantiates with expected values")
    void tickerListInstantiatesWithExpectedValues() {
        RoomTicker first = new RoomTicker("alice", "one");
        RoomTicker second = new RoomTicker("bob", "two");
        List<RoomTicker> tickers = new ArrayList<>(List.of(first, second));
        RoomTickerListReceivedEvent args = new RoomTickerListReceivedEvent("lobby", tickers);

        tickers.clear();

        assertEquals("lobby", args.roomName());
        assertEquals(2, args.tickerCount());
        assertEquals(List.of(first, second), args.tickers());
        assertThrows(UnsupportedOperationException.class, () -> args.tickers().add(new RoomTicker("carol", "three")));
    }

    @Test
    @DisplayName("RoomTickerListReceivedEvent treats null tickers as empty")
    void tickerListTreatsNullAsEmpty() {
        RoomTickerListReceivedEvent args = new RoomTickerListReceivedEvent("lobby", null);

        assertEquals(0, args.tickerCount());
        assertEquals(List.of(), args.tickers());
    }

    @Test
    @DisplayName("Room ticker arguments preserve nullable references")
    void tickerArgumentsPreserveNullableReferences() {
        RoomTickerAddedEvent added = new RoomTickerAddedEvent(null, null);
        RoomTickerRemovedEvent removed = new RoomTickerRemovedEvent(null, null);

        assertNull(added.roomName());
        assertNull(added.ticker());
        assertNull(removed.username());
    }
}
