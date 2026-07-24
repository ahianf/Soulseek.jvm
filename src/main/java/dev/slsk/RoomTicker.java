// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

/**
 * A chat-room ticker.
 */
public class RoomTicker {
    private final String message;
    private final String username;

    /**
     * Creates a room ticker.
     *
     * @param username the username to which the ticker belongs
     * @param message the ticker message
     */
    public RoomTicker(String username, String message) {
        this.username = username;
        this.message = message;
    }

    /**
     * Returns the username to which the ticker belongs.
     *
     * @return the username
     */
    public final String getUsername() {
        return username;
    }

    /**
     * Returns the ticker message.
     *
     * @return the ticker message
     */
    public final String getMessage() {
        return message;
    }
}
