// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.Objects;

/**
 * Whether a user is around, and whether the server treats them as privileged.
 *
 * <p>A user being offline is a value, not a failure. Asking after somebody who
 * is not there returns {@link UserPresence#OFFLINE} rather than throwing,
 * because "are they online?" answered with "no" is the question working, not
 * failing.
 *
 * @param user who this describes
 * @param presence whether they are around
 * @param privileged whether the server grants them queue precedence, which is
 *     protocol-mandated rather than a matter of taste
 */
public record UserStatus(Username user, UserPresence presence, boolean privileged) {

    /** Validates and returns the status. */
    public UserStatus {
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(presence, "presence");
    }

    /**
     * Returns whether the user is reachable at all.
     *
     * @return {@code true} unless offline
     */
    public boolean isOnline() {
        return presence != UserPresence.OFFLINE;
    }
}
