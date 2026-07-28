// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.spi;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/** The default {@link TransferSink#file} implementation. */
final class FileTransferSink implements TransferSink {

    private final Path destination;
    private final Path partial;
    private FileChannel channel;

    FileTransferSink(Path destination) {
        this.destination = Objects.requireNonNull(destination, "destination");
        this.partial = destination.resolveSibling(destination.getFileName() + ".part");
    }

    @Override
    public WritableByteChannel open(long resumeOffset) throws IOException {
        if (resumeOffset < 0) {
            throw new IllegalArgumentException("resumeOffset must not be negative: " + resumeOffset);
        }
        Path parent = partial.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        channel = FileChannel.open(partial, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        // Anything past the offset is from an attempt that did not finish and
        // is not known to be the same bytes; the peer is about to send them
        // again. Truncating first means a short resume cannot leave stale tail
        // bytes behind a shorter file.
        channel.truncate(resumeOffset);
        channel.position(resumeOffset);
        return channel;
    }

    @Override
    public void commit() throws IOException {
        close();
        Path parent = destination.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        try {
            Files.move(partial, destination, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException unsupported) {
            // Some filesystems cannot promise it. A replacing move is still
            // closer to atomic than writing the destination in place would be.
            Files.move(partial, destination, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Override
    public void discard() {
        try {
            close();
        } catch (IOException ignored) {
            // Discard runs from a path that has already failed once. There is
            // nobody left to tell, and the part file is still on disk either
            // way.
        }
    }

    private void close() throws IOException {
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }
}
