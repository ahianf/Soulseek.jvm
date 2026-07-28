// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.Objects;

/**
 * What the server reports about a user's sharing, which is what an upload
 * policy weighs when deciding whether to serve them.
 *
 * @param user who this describes
 * @param averageSpeed their average upload speed in bytes per second
 * @param uploadCount how many files they have uploaded
 * @param fileCount how many files they share
 * @param directoryCount how many directories they share
 */
public record UserStatistics(Username user, int averageSpeed, long uploadCount, int fileCount, int directoryCount) {

    /** Validates and returns the statistics. */
    public UserStatistics {
        Objects.requireNonNull(user, "user");
    }

    /**
     * Returns whether the user shares nothing.
     *
     * <p>A common upload-policy input: a peer sharing nothing is one most
     * clients decline to serve.
     *
     * @return {@code true} if they share no files
     */
    public boolean sharesNothing() {
        return fileCount <= 0;
    }
}
