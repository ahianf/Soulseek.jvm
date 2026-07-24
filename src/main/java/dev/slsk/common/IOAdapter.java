// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.common;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Testable adapter around Java filesystem I/O.
 */
public class IOAdapter {
    /**
     * Returns whether a path exists.
     *
     * @param path the path to check
     * @return whether it exists
     */
    public boolean exists(String path) {
        return Files.exists(Path.of(path));
    }

    /**
     * Opens a seekable file channel.
     *
     * @param path the path to open
     * @param options the open options
     * @return the file channel
     * @throws IOException when the file cannot be opened
     */
    public FileChannel getFileChannel(String path, OpenOption... options) throws IOException {
        return FileChannel.open(Path.of(path), options);
    }

    /**
     * Reads basic file metadata.
     *
     * @param path the path to inspect
     * @return the basic attributes
     * @throws IOException when metadata cannot be read
     */
    public BasicFileAttributes getFileInfo(String path) throws IOException {
        return Files.readAttributes(Path.of(path), BasicFileAttributes.class);
    }
}
