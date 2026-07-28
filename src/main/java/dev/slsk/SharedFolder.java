// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.nio.file.Path;
import java.util.Objects;

/**
 * A local directory offered to the network.
 *
 * @param path the directory
 * @param locked whether its contents are reserved for privileged users
 */
public record SharedFolder(Path path, boolean locked) {

    /** Validates and returns the folder. */
    public SharedFolder {
        Objects.requireNonNull(path, "path");
    }

    /**
     * Returns an ordinary shared folder.
     *
     * @param path the directory
     * @return the folder
     */
    public static SharedFolder of(Path path) {
        return new SharedFolder(path, false);
    }

    /**
     * Returns a folder whose contents only privileged users may take.
     *
     * @param path the directory
     * @return the folder
     */
    public static SharedFolder locked(Path path) {
        return new SharedFolder(path, true);
    }
}
