// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

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

class RoomEventArgsTest {
    @Test
    @DisplayName("RoomJoinedEventArgs instantiates with expected values")
    void joinedInstantiatesWithExpectedValues() {
        UserData userData = new UserData("alice", UserPresence.ONLINE, 1, 2, 3, 4, "CL");
        RoomJoinedEventArgs args = new RoomJoinedEventArgs("lobby", "alice", userData);

        assertEquals("lobby", args.getRoomName());
        assertEquals("alice", args.getUsername());
        assertSame(userData, args.getUserData());
    }

    @Test
    @DisplayName("RoomLeftEventArgs instantiates with expected values")
    void leftInstantiatesWithExpectedValues() {
        RoomLeftEventArgs args = new RoomLeftEventArgs("lobby", "alice");

        assertEquals("lobby", args.getRoomName());
        assertEquals("alice", args.getUsername());
    }

    @Test
    @DisplayName("RoomMessageReceivedEventArgs instantiates with expected values")
    void messageInstantiatesWithExpectedValues() {
        RoomMessageReceivedEventArgs args = new RoomMessageReceivedEventArgs("lobby", "alice", "hello");

        assertEquals("lobby", args.getRoomName());
        assertEquals("alice", args.getUsername());
        assertEquals("hello", args.getMessage());
    }

    @Test
    @DisplayName("Room event arguments preserve nullable references")
    void preservesNullableReferences() {
        RoomJoinedEventArgs joined = new RoomJoinedEventArgs(null, null, null);
        RoomMessageReceivedEventArgs message = new RoomMessageReceivedEventArgs(null, null, null);

        assertNull(joined.getRoomName());
        assertNull(joined.getUsername());
        assertNull(joined.getUserData());
        assertNull(message.getMessage());
    }

    @Test
    @DisplayName("RoomTickerAddedEventArgs instantiates with expected values")
    void tickerAddedInstantiatesWithExpectedValues() {
        RoomTicker ticker = new RoomTicker("alice", "hello");
        RoomTickerAddedEventArgs args = new RoomTickerAddedEventArgs("lobby", ticker);

        assertEquals("lobby", args.getRoomName());
        assertSame(ticker, args.getTicker());
    }

    @Test
    @DisplayName("RoomTickerRemovedEventArgs instantiates with expected values")
    void tickerRemovedInstantiatesWithExpectedValues() {
        RoomTickerRemovedEventArgs args = new RoomTickerRemovedEventArgs("lobby", "alice");

        assertEquals("lobby", args.getRoomName());
        assertEquals("alice", args.getUsername());
    }

    @Test
    @DisplayName("RoomTickerListReceivedEventArgs instantiates with expected values")
    void tickerListInstantiatesWithExpectedValues() {
        RoomTicker first = new RoomTicker("alice", "one");
        RoomTicker second = new RoomTicker("bob", "two");
        List<RoomTicker> tickers = new ArrayList<>(List.of(first, second));
        RoomTickerListReceivedEventArgs args = new RoomTickerListReceivedEventArgs("lobby", tickers);

        tickers.clear();

        assertEquals("lobby", args.getRoomName());
        assertEquals(2, args.getTickerCount());
        assertEquals(List.of(first, second), args.getTickers());
        assertThrows(
                UnsupportedOperationException.class, () -> args.getTickers().add(new RoomTicker("carol", "three")));
    }

    @Test
    @DisplayName("RoomTickerListReceivedEventArgs treats null tickers as empty")
    void tickerListTreatsNullAsEmpty() {
        RoomTickerListReceivedEventArgs args = new RoomTickerListReceivedEventArgs("lobby", null);

        assertEquals(0, args.getTickerCount());
        assertEquals(List.of(), args.getTickers());
    }

    @Test
    @DisplayName("Room ticker arguments preserve nullable references")
    void tickerArgumentsPreserveNullableReferences() {
        RoomTickerAddedEventArgs added = new RoomTickerAddedEventArgs(null, null);
        RoomTickerRemovedEventArgs removed = new RoomTickerRemovedEventArgs(null, null);

        assertNull(added.getRoomName());
        assertNull(added.getTicker());
        assertNull(removed.getUsername());
    }
}
