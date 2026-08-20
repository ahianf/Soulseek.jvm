// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Test seam around Java filesystem access.
 */
public class FileSystemAccess {
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
    public FileChannel openChannel(String path, OpenOption... options) throws IOException {
        return FileChannel.open(Path.of(path), options);
    }

    /**
     * Opens a seekable file input stream.
     *
     * @param path the path to open
     * @return the input stream
     * @throws IOException when the file cannot be opened
     */
    public InputStream openInputStream(String path) throws IOException {
        return new FileInputStream(path);
    }

    /**
     * Opens a file output stream.
     *
     * @param path the path to open
     * @param append whether to append rather than overwrite
     * @return the output stream
     * @throws IOException when the file cannot be opened
     */
    public OutputStream openOutputStream(String path, boolean append) throws IOException {
        return new FileOutputStream(path, append);
    }

    /**
     * Reads basic file metadata.
     *
     * @param path the path to inspect
     * @return the basic attributes
     * @throws IOException when metadata cannot be read
     */
    public BasicFileAttributes readAttributes(String path) throws IOException {
        return Files.readAttributes(Path.of(path), BasicFileAttributes.class);
    }
}
