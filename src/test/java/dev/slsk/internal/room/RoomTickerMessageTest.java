// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.room;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RoomTickerMessageTest {
    @Test
    @DisplayName("Instantiates with the given data")
    void instantiatesWithTheGivenData() {
        RoomTickerMessage ticker = new RoomTickerMessage("alice", "hello");

        assertEquals("alice", ticker.username());
        assertEquals("hello", ticker.message());
    }

    @Test
    @DisplayName("Preserves null data")
    void preservesNullData() {
        RoomTickerMessage ticker = new RoomTickerMessage(null, null);

        assertNull(ticker.username());
        assertNull(ticker.message());
    }
}
