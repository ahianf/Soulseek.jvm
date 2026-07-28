// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import java.util.Objects;

/**
 * User status.
 */
public class UserStatus {
    private final boolean privileged;
    private final UserPresence presence;
    private final String username;

    /**
     * Creates a user status.
     *
     * @param username the username
     * @param presence the user's network presence
     * @param isPrivileged whether the user is privileged
     */
    public UserStatus(String username, UserPresence presence, boolean isPrivileged) {
        this.username = username;
        this.presence = Objects.requireNonNull(presence, "presence");
        this.privileged = isPrivileged;
    }

    /**
     * Returns whether the user is privileged.
     *
     * @return whether the user is privileged
     */
    public final boolean isPrivileged() {
        return privileged;
    }

    /**
     * Returns the user's network presence.
     *
     * @return the user's network presence
     */
    public final UserPresence getPresence() {
        return presence;
    }

    /**
     * Returns the username.
     *
     * @return the username
     */
    public final String getUsername() {
        return username;
    }
}
