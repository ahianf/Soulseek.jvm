// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.user.Username;
import java.util.List;
import java.util.Objects;

/**
 * One peer's answer to a search.
 *
 * <p>Responses are handed over exactly as they arrived. The library does not
 * group them by user, deduplicate across them, rank "best source", or sort:
 * those are presentation decisions, every application makes them differently,
 * and a library that picked one would be wrong for the rest and impossible to
 * override.
 *
 * @param user who answered
 * @param freeUploadSlots how many upload slots they have free
 * @param uploadSpeed their average upload speed, in bytes per second
 * @param queueLength how many transfers are waiting in their queue
 * @param files what they offered
 * @param lockedFiles files they hold back for privileged users
 */
public record SearchResponse(
        Username user,
        int freeUploadSlots,
        long uploadSpeed,
        int queueLength,
        List<SearchFile> files,
        List<SearchFile> lockedFiles) {

    /** Validates and returns the response. */
    public SearchResponse {
        Objects.requireNonNull(user, "user");
        files = List.copyOf(Objects.requireNonNull(files, "files"));
        lockedFiles = List.copyOf(Objects.requireNonNull(lockedFiles, "lockedFiles"));
    }

    /**
     * Returns how many files this peer offered, locked ones included.
     *
     * @return the file count
     */
    public int fileCount() {
        return files.size() + lockedFiles.size();
    }

    /**
     * Returns whether this peer can start a transfer now.
     *
     * @return {@code true} if a slot is free
     */
    public boolean hasFreeSlot() {
        return freeUploadSlots > 0;
    }
}
