// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.user;

import java.util.Objects;

/**
 * How far a browse has got.
 *
 * <p>A browse is one message, and a well-shared account's is measured in
 * megabytes: two hundred thousand entries is ordinary. A consumer that shows no
 * progress for that long looks hung, which is why the request carries a
 * progress callback rather than the library assuming nobody is watching.
 *
 * @param user whose share is being read
 * @param transferred bytes received so far
 * @param total bytes expected, which the peer declares up front
 */
public record BrowseProgress(Username user, long transferred, long total) {

    /** Validates and returns the progress. */
    public BrowseProgress {
        Objects.requireNonNull(user, "user");
    }

    /**
     * Returns how far along the browse is, from zero to one.
     *
     * @return the fraction complete, or zero while the size is unknown
     */
    public double fraction() {
        return total <= 0 ? 0 : Math.min(1.0, (double) transferred / total);
    }
}
