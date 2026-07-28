// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

/**
 * Event arguments raised when a ticker is removed from a chat room.
 */
public class RoomTickerRemovedEvent extends RoomTickerEvent {
    private final String username;

    /**
     * Creates ticker-removed event payload.
     *
     * @param roomName the room from which the ticker was removed
     * @param username the user to whom the ticker belonged
     */
    public RoomTickerRemovedEvent(String roomName, String username) {
        super(roomName);
        this.username = username;
    }

    /**
     * Returns the user to whom the ticker belonged.
     *
     * @return the username
     */
    public final String getUsername() {
        return username;
    }
}
