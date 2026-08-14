// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.user;

import dev.slsk.Directory;
import dev.slsk.search.SearchFile;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * What one user was sharing, at one moment.
 *
 * <p><strong>Flat, not a tree.</strong> The goal sketched a {@code
 * DirectoryTree} with a root, and the protocol has no such thing: a browse
 * response is a flat list of directories, each carrying its full
 * backslash-joined path, and any hierarchy a user sees is something a client
 * inferred. Building the tree here would hand every consumer the same
 * guesses — about a separator inside a name, about a directory that is
 * implied by a child but never listed, about which of two roots is the root —
 * and those guesses would then be part of the public surface. What the sketch
 * actually needed was the counts, a lookup by path, and a lazy walk of the
 * files; those are here, and a consumer that wants a tree can build the one
 * its own display needs.
 *
 * @param user whose share this is
 * @param at when it was read
 * @param directories what they are sharing
 * @param lockedDirectories what they listed but will not serve
 */
public record Browse(Username user, Instant at, List<Directory> directories, List<Directory> lockedDirectories) {

    /** Validates and returns the browse. */
    public Browse {
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(at, "at");
        directories = List.copyOf(Objects.requireNonNull(directories, "directories"));
        lockedDirectories = List.copyOf(Objects.requireNonNull(lockedDirectories, "lockedDirectories"));
    }

    /**
     * Returns how many directories are listed, locked ones included.
     *
     * @return the directory count
     */
    public int directoryCount() {
        return directories.size() + lockedDirectories.size();
    }

    /**
     * Returns how many files are listed, locked ones included.
     *
     * @return the file count
     */
    public int fileCount() {
        return (int) files().count();
    }

    /**
     * Returns the total size of every file listed.
     *
     * @return the total in bytes
     */
    public long totalBytes() {
        return files().mapToLong(SearchFile::size).sum();
    }

    /**
     * Returns one directory by its full remote path.
     *
     * @param path the directory's full remote path
     * @return the directory, or empty if it was not listed
     */
    public Optional<Directory> at(String path) {
        return Stream.concat(directories.stream(), lockedDirectories.stream())
                .filter(directory -> directory.name().equals(path))
                .findFirst();
    }

    /**
     * Returns every file listed, lazily.
     *
     * <p>Lazy because a well-shared account's browse carries two hundred
     * thousand entries, and a consumer looking for one extension should not
     * have to materialise the rest.
     *
     * @return the files
     */
    public Stream<SearchFile> files() {
        return Stream.concat(directories.stream(), lockedDirectories.stream()).flatMap(d -> d.files().stream());
    }
}
