// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.List;
import java.util.Objects;

/**
 * Everything an account shares, as it is handed to one peer who asked.
 *
 * <p>Locked directories are listed but not served: they exist so a peer can see
 * that there is more, which is how the network's private-share convention
 * works. A catalog that does not use the convention returns an empty list and
 * loses nothing.
 *
 * @param directories what the requester may fetch from
 * @param lockedDirectories what they may see but not fetch
 */
public record BrowseResponse(List<Directory> directories, List<Directory> lockedDirectories) {

    /** Validates and returns the response. */
    public BrowseResponse {
        directories = List.copyOf(Objects.requireNonNull(directories, "directories"));
        lockedDirectories = List.copyOf(Objects.requireNonNull(lockedDirectories, "lockedDirectories"));
    }

    /**
     * Returns a response sharing nothing.
     *
     * @return the empty response
     */
    public static BrowseResponse empty() {
        return new BrowseResponse(List.of(), List.of());
    }

    /**
     * Returns a response with nothing locked.
     *
     * @param directories what the requester may fetch from
     * @return the response
     */
    public static BrowseResponse of(List<Directory> directories) {
        return new BrowseResponse(directories, List.of());
    }

    /**
     * Returns how many files are on offer, locked ones included.
     *
     * @return the file count
     */
    public int fileCount() {
        return directories.stream().mapToInt(Directory::fileCount).sum()
                + lockedDirectories.stream().mapToInt(Directory::fileCount).sum();
    }
}
