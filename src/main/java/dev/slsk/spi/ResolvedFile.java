// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.spi;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * A file this account is sharing, resolved to something the library can send.
 *
 * <p>{@code UploadStreamFactory} asked the application for an {@code
 * InputStream} and a size, separately, with nothing tying them together and no
 * way to say "I do not have that file" other than by failing a future. A peer
 * asking for a file it cannot have is an ordinary event on this network, not an
 * exception, which is why {@link ShareCatalog#resolve} returns an {@link
 * java.util.Optional} of this rather than throwing.
 *
 * <p>{@link #open} takes an offset because a peer may resume, and answering that
 * by opening at zero and discarding bytes is the kind of thing a library should
 * not make an application think about.
 */
public interface ResolvedFile {

    /**
     * Returns the file's size in bytes.
     *
     * @return the size
     */
    long size();

    /**
     * Opens the file for reading at an offset.
     *
     * @param offset where the peer wants the bytes to start
     * @return the channel to read from
     * @throws IOException if the file cannot be opened
     */
    ReadableByteChannel open(long offset) throws IOException;

    /**
     * Returns a resolved file backed by a local path.
     *
     * @param source the local file
     * @return the resolved file
     * @throws IOException if its size cannot be read
     */
    static ResolvedFile of(Path source) throws IOException {
        Objects.requireNonNull(source, "source");
        long size = Files.size(source);
        return new ResolvedFile() {
            @Override
            public long size() {
                return size;
            }

            @Override
            public ReadableByteChannel open(long offset) throws IOException {
                FileChannel channel = FileChannel.open(source, StandardOpenOption.READ);
                try {
                    channel.position(offset);
                } catch (IOException | RuntimeException failure) {
                    channel.close();
                    throw failure;
                }
                return channel;
            }
        };
    }
}
