// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.messaging.messages.RoomTickerListNotification;
import dev.slsk.internal.room.RoomTickerMessage;
import java.util.List;

/** Event payload emitted when a chat-room ticker list is received. */
public record RoomTickerListReceivedEvent(String roomName, List<RoomTickerMessage> tickers)
        implements SoulseekClientEvent {

    /**
     * Creates ticker-list event payload.
     *
     * @param roomName the room to which the list applies
     * @param tickers the tickers, or {@code null} for an empty list
     */
    public RoomTickerListReceivedEvent {
        tickers = tickers == null ? List.of() : List.copyOf(tickers);
    }

    /**
     * Creates event payload from an internal protocol notification.
     *
     * @param notification the notification that raised the event
     */
    public RoomTickerListReceivedEvent(RoomTickerListNotification notification) {
        this(notification.roomName(), notification.tickers());
    }

    /**
     * Returns the number of tickers.
     *
     * @return the ticker count
     */
    public int tickerCount() {
        return tickers.size();
    }
}
