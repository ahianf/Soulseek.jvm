// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.internal.options.PositionableOutputStream;
import dev.slsk.spi.TransferSink;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;

/**
 * A {@link TransferSink} seen as the transfer engine sees a download stream.
 *
 * <p>The engine writes to an {@code OutputStream} and asks it where it is; a
 * sink hands out a channel and is told where to start. The gap between them is
 * arithmetic: the sink was opened at an offset, so the position is that offset
 * plus what has been written since.
 *
 * <p>Seeking is answered rather than performed. {@link #setPosition} exists
 * because the engine's automatic-resume path calls it, and the sink has already
 * done the seek in {@code open(offset)}; asking for the position it is already
 * at is a no-op, and asking for any other is a bug worth hearing about rather
 * than a silent write to the wrong place.
 */
final class SinkOutputStream extends OutputStream implements PositionableOutputStream {

    private final TransferSink sink;
    private final long offset;
    private final WritableByteChannel channel;
    private long written;

    SinkOutputStream(TransferSink sink, long resumeOffset) throws IOException {
        this.sink = Objects.requireNonNull(sink, "sink");
        this.offset = resumeOffset;
        this.channel = Objects.requireNonNull(sink.open(resumeOffset), "sink.open");
    }

    @Override
    public void write(int value) throws IOException {
        write(new byte[] {(byte) value}, 0, 1);
    }

    @Override
    public void write(byte[] bytes, int start, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes, start, length);
        while (buffer.hasRemaining()) {
            int count = channel.write(buffer);
            if (count <= 0) {
                throw new IOException("the transfer sink accepted no bytes");
            }
            written += count;
        }
    }

    @Override
    public long getPosition() {
        return offset + written;
    }

    @Override
    public void setPosition(long position) throws IOException {
        if (position != getPosition()) {
            throw new IOException(
                    "a transfer sink cannot seek after opening; opened at " + offset + ", asked for " + position);
        }
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
            // The sink is about to be told the attempt failed, which is the
            // only party that could act on this.
        }
        sink.discard();
    }
}
