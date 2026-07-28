// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.List;
import java.util.Objects;

/**
 * A directory of shared files, as one side of the network describes it to the
 * other.
 *
 * <p>Soulseek has no directory tree on the wire. A browse response is a flat
 * list of these, each carrying its full backslash-joined path as its name, and
 * whatever hierarchy a user sees is something the client inferred. Keeping the
 * flat shape here is deliberate: inventing a tree in the library would mean
 * every consumer inheriting the same guesses about a path that the peer never
 * meant as a tree.
 *
 * @param name the directory's full remote path
 * @param files what it contains
 */
public record Directory(String name, List<SearchFile> files) {

    /** Validates and returns the directory. */
    public Directory {
        Objects.requireNonNull(name, "name");
        files = List.copyOf(Objects.requireNonNull(files, "files"));
    }

    /**
     * Returns an empty directory.
     *
     * @param name the directory's full remote path
     * @return the directory
     */
    public static Directory of(String name) {
        return new Directory(name, List.of());
    }

    /**
     * Returns how many files it contains.
     *
     * @return the file count
     */
    public int fileCount() {
        return files.size();
    }

    /**
     * Returns the directory's own name, without the directories above it.
     *
     * @return the last path segment
     */
    public String simpleName() {
        return RemotePath.lastFolderSegment(name);
    }
}
