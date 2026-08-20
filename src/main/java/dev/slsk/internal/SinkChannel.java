// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.spi.TransferSink;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;

/** A transfer sink channel with commit/discard ownership and position tracking. */
final class SinkChannel implements WritableByteChannel {

    private final TransferSink sink;
    private final long offset;
    private final WritableByteChannel channel;
    private long written;

    SinkChannel(TransferSink sink, long resumeOffset) throws IOException {
        this.sink = Objects.requireNonNull(sink, "sink");
        this.offset = resumeOffset;
        this.channel = Objects.requireNonNull(sink.open(resumeOffset), "sink.open");
    }

    @Override
    public int write(ByteBuffer source) throws IOException {
        int count = channel.write(source);
        if (count <= 0 && source.hasRemaining()) {
            throw new IOException("the transfer sink accepted no bytes");
        }
        written += count;
        return count;
    }

    long position() {
        return offset + written;
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    /** Makes the download visible. Called once, on success. */
    void commit() throws IOException {
        close();
        sink.commit();
    }

    /** Abandons the attempt. Never throws. */
    void discard() {
        try {
            close();
        } catch (IOException ignored) {
            // The sink is about to be told the attempt failed.
        }
        sink.discard();
    }
}
