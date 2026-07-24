// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

/**
 * Event arguments raised when a ticker is removed from a chat room.
 */
public class RoomTickerRemovedEventArgs extends RoomTickerEventArgs {
    private final String username;

    /**
     * Creates ticker-removed event arguments.
     *
     * @param roomName the room from which the ticker was removed
     * @param username the user to whom the ticker belonged
     */
    public RoomTickerRemovedEventArgs(String roomName, String username) {
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
