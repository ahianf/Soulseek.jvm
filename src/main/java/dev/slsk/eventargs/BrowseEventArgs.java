// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

/**
 * Event arguments for browse events.
 */
public class BrowseEventArgs extends SoulseekClientEventArgs {
    private final String username;

    /**
     * Creates browse event arguments.
     *
     * @param username the user associated with the event
     */
    public BrowseEventArgs(String username) {
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
