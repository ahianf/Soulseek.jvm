// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

/**
 * Base event payload for events associated with a user.
 */
public abstract class UserEvent extends SoulseekClientEvent {
    private final String username;

    /**
     * Creates user event payload.
     *
     * @param username the associated username
     */
    protected UserEvent(String username) {
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
