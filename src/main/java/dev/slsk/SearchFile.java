// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.Objects;

/**
 * A file a peer offered in answer to a search.
 *
 * @param path where it lives on the peer, in Soulseek's backslash-joined form
 * @param size its size in bytes
 * @param attributes what the peer said about it
 */
public record SearchFile(String path, long size, FileAttributes attributes) {

    /** Validates and returns the file. */
    public SearchFile {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(attributes, "attributes");
    }

    /**
     * Returns the file name, without the directories above it.
     *
     * @return the last path segment
     */
    public String name() {
        return RemotePath.basename(path);
    }

    /**
     * Returns the file extension, lowercased, without the dot.
     *
     * @return the extension, or empty if it has none
     */
    public String extension() {
        String name = name();
        int dot = name.lastIndexOf('.');
        return dot < 0 || dot == name.length() - 1
                ? ""
                : name.substring(dot + 1).toLowerCase(java.util.Locale.ROOT);
    }
}
