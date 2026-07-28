// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RoomTickerTest {
    @Test
    @DisplayName("Instantiates with the given data")
    void instantiatesWithTheGivenData() {
        RoomTicker ticker = new RoomTicker("alice", "hello");

        assertEquals("alice", ticker.getUsername());
        assertEquals("hello", ticker.getMessage());
    }

    @Test
    @DisplayName("Preserves null data")
    void preservesNullData() {
        RoomTicker ticker = new RoomTicker(null, null);

        assertNull(ticker.getUsername());
        assertNull(ticker.getMessage());
    }
}
