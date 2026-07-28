// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

/**
 * Event payload for browse events.
 */
public class BrowseEvent extends SoulseekClientEvent {
    private final String username;

    /**
     * Creates browse event payload.
     *
     * @param username the user associated with the event
     */
    public BrowseEvent(String username) {
        this.username = username;
    }

    /**
     * Returns the user associated with the event.
     *
     * @return the username
     */
    public final String getUsername() {
        return username;
    }
}
