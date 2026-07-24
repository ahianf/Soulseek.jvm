// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

/**
 * Base event arguments for events associated with a user.
 */
public abstract class UserEventArgs extends SoulseekClientEventArgs {
    private final String username;

    /**
     * Creates user event arguments.
     *
     * @param username the associated username
     */
    protected UserEventArgs(String username) {
        this.username = username;
    }

    /**
     * Returns the associated username.
     *
     * @return the username
     */
    public final String getUsername() {
        return username;
    }
}
