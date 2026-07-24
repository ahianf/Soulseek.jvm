// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import dev.slsk.RoomTicker;

/**
 * Event arguments raised when a ticker is added to a chat room.
 */
public class RoomTickerAddedEventArgs extends RoomTickerEventArgs {
    private final RoomTicker ticker;

    /**
     * Creates ticker-added event arguments.
     *
     * @param roomName the room to which the ticker was added
     * @param ticker the ticker
     */
    public RoomTickerAddedEventArgs(String roomName, RoomTicker ticker) {
        super(roomName);
        this.ticker = ticker;
    }

    /**
     * Returns the added ticker.
     *
     * @return the ticker
     */
    public final RoomTicker getTicker() {
        return ticker;
    }
}
