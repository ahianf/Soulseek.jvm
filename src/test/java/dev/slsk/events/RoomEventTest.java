// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.RoomTicker;
import dev.slsk.UserData;
import dev.slsk.UserPresence;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RoomEventTest {
    @Test
    @DisplayName("RoomJoinedEvent instantiates with expected values")
    void joinedInstantiatesWithExpectedValues() {
        UserData userData = new UserData("alice", UserPresence.ONLINE, 1, 2, 3, 4, "CL");
        RoomJoinedEvent args = new RoomJoinedEvent("lobby", "alice", userData);

        assertEquals("lobby", args.getRoomName());
        assertEquals("alice", args.getUsername());
        assertSame(userData, args.getUserData());
    }

    @Test
    @DisplayName("RoomLeftEvent instantiates with expected values")
    void leftInstantiatesWithExpectedValues() {
        RoomLeftEvent args = new RoomLeftEvent("lobby", "alice");

        assertEquals("lobby", args.getRoomName());
        assertEquals("alice", args.getUsername());
    }

    @Test
    @DisplayName("RoomMessageReceivedEvent instantiates with expected values")
    void messageInstantiatesWithExpectedValues() {
        RoomMessageReceivedEvent args = new RoomMessageReceivedEvent("lobby", "alice", "hello");

        assertEquals("lobby", args.getRoomName());
        assertEquals("alice", args.getUsername());
        assertEquals("hello", args.getMessage());
    }

    @Test
    @DisplayName("Room event arguments preserve nullable references")
    void preservesNullableReferences() {
        RoomJoinedEvent joined = new RoomJoinedEvent(null, null, null);
        RoomMessageReceivedEvent message = new RoomMessageReceivedEvent(null, null, null);

        assertNull(joined.getRoomName());
        assertNull(joined.getUsername());
        assertNull(joined.getUserData());
        assertNull(message.getMessage());
    }

    @Test
    @DisplayName("RoomTickerAddedEvent instantiates with expected values")
    void tickerAddedInstantiatesWithExpectedValues() {
        RoomTicker ticker = new RoomTicker("alice", "hello");
        RoomTickerAddedEvent args = new RoomTickerAddedEvent("lobby", ticker);

        assertEquals("lobby", args.getRoomName());
        assertSame(ticker, args.getTicker());
    }

    @Test
    @DisplayName("RoomTickerRemovedEvent instantiates with expected values")
    void tickerRemovedInstantiatesWithExpectedValues() {
        RoomTickerRemovedEvent args = new RoomTickerRemovedEvent("lobby", "alice");

        assertEquals("lobby", args.getRoomName());
        assertEquals("alice", args.getUsername());
    }

    @Test
    @DisplayName("RoomTickerListReceivedEvent instantiates with expected values")
    void tickerListInstantiatesWithExpectedValues() {
        RoomTicker first = new RoomTicker("alice", "one");
        RoomTicker second = new RoomTicker("bob", "two");
        List<RoomTicker> tickers = new ArrayList<>(List.of(first, second));
        RoomTickerListReceivedEvent args = new RoomTickerListReceivedEvent("lobby", tickers);

        tickers.clear();

        assertEquals("lobby", args.getRoomName());
        assertEquals(2, args.getTickerCount());
        assertEquals(List.of(first, second), args.getTickers());
        assertThrows(
                UnsupportedOperationException.class, () -> args.getTickers().add(new RoomTicker("carol", "three")));
    }

    @Test
    @DisplayName("RoomTickerListReceivedEvent treats null tickers as empty")
    void tickerListTreatsNullAsEmpty() {
        RoomTickerListReceivedEvent args = new RoomTickerListReceivedEvent("lobby", null);

        assertEquals(0, args.getTickerCount());
        assertEquals(List.of(), args.getTickers());
    }

    @Test
    @DisplayName("Room ticker arguments preserve nullable references")
    void tickerArgumentsPreserveNullableReferences() {
        RoomTickerAddedEvent added = new RoomTickerAddedEvent(null, null);
        RoomTickerRemovedEvent removed = new RoomTickerRemovedEvent(null, null);

        assertNull(added.getRoomName());
        assertNull(added.getTicker());
        assertNull(removed.getUsername());
    }
}
