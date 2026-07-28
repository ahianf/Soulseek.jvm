// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.RoomTicker;
import dev.slsk.internal.messaging.messages.RoomTickerListNotification;
import java.util.ArrayList;
import java.util.List;

/**
 * Event arguments raised when a chat-room ticker list is received.
 */
public class RoomTickerListReceivedEvent extends RoomTickerEvent {
    private final List<RoomTicker> tickers;

    /**
     * Creates ticker-list event payload.
     *
     * @param roomName the room to which the list applies
     * @param tickers the tickers, or {@code null} for an empty list
     */
    public RoomTickerListReceivedEvent(String roomName, Iterable<? extends RoomTicker> tickers) {
        super(roomName);

        if (tickers == null) {
            this.tickers = List.of();
            return;
        }

        ArrayList<RoomTicker> copy = new ArrayList<>();
        tickers.forEach(copy::add);
        this.tickers = java.util.Collections.unmodifiableList(copy);
    }

    /**
     * Creates event payload from an internal protocol notification.
     *
     * @param notification the notification that raised the event
     */
    public RoomTickerListReceivedEvent(RoomTickerListNotification notification) {
        this(notification.getRoomName(), notification.getTickers());
    }

    /**
     * Returns the number of tickers.
     *
     * @return the ticker count
     */
    public final int getTickerCount() {
        return tickers.size();
    }

    /**
     * Returns an immutable snapshot of the tickers.
     *
     * @return the tickers
     */
    public final List<RoomTicker> getTickers() {
        return tickers;
    }
}
