// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

import dev.slsk.internal.options.PositionableInputStream;
import dev.slsk.internal.options.PositionableOutputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;

/**
 * Positioning a stream, and knowing where it got to.
 *
 * <p>Both directions of a transfer need the same four things: seek a stream to
 * the offset the peer and we agreed on, find out where it actually is, count
 * what has moved through it, and read a filename out of a Soulseek path for a
 * diagnostic. None of that is a decision about a transfer, so none of it
 * belongs in the domain that makes those; it sat there because it started life
 * inside the class that did everything.
 */
public final class TransferStreams {

    private TransferStreams() {}

    public static final class PositionTrackingInputStream extends FilterInputStream {
        private long position;

        public PositionTrackingInputStream(InputStream inputStream, long initialPosition) {
            super(inputStream);
            position = initialPosition;
        }

        public long getPosition() {
            return position;
        }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value >= 0) {
                position++;
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int read = super.read(bytes, offset, length);
            if (read > 0) {
                position += read;
            }
            return read;
        }

        @Override
        public long skip(long count) throws IOException {
            long skipped = super.skip(count);
            position += skipped;
            return skipped;
        }
    }

    /**
     * Adapts a channel that was opened at an offset to a stream that knows it.
     *
     * <p>{@link java.nio.channels.Channels#newInputStream} alone is not enough
     * here. It returns a stream of a JDK-internal type, which is none of the
     * three {@link #seekInputStream} can position and none of the two {@link
     * #determinePosition} can interrogate — so a resumed upload asked to seek
     * one threw "input stream is not seekable" and failed every time, while
     * every upload starting at zero passed straight through the early return
     * and worked. That made resume the one broken case and hid it behind the
     * fifteen that were not.
     *
     * <p>Which is doubly wrong, because the channel arrives <em>already</em> at
     * the offset: {@link dev.slsk.spi.ResolvedFile#open} takes one for
     * exactly that reason. So the seek was never work, only a check — and this
     * is what lets it be one. A seekable channel answers from the channel; a
     * channel that cannot seek answers with where it was opened, which is the
     * truth it has and all the check needs.
     *
     * @param channel the channel to read from
     * @param offset where it was opened
     * @return a stream that reports and honours that position
     */
    public static InputStream positionedStream(ReadableByteChannel channel, long offset) {
        return new PositionedChannelStream(channel, offset);
    }

    private static final class PositionedChannelStream extends FilterInputStream implements PositionableInputStream {
        private final ReadableByteChannel channel;
        private final long openedAt;

        PositionedChannelStream(ReadableByteChannel channel, long openedAt) {
            super(Channels.newInputStream(channel));
            this.channel = channel;
            this.openedAt = openedAt;
        }

        @Override
        public long getPosition() throws IOException {
            // Reads through the stream advance the channel, so a seekable one
            // stays the better answer for as long as it is open.
            return channel instanceof SeekableByteChannel seekable ? seekable.position() : openedAt;
        }

        @Override
        public void setPosition(long position) throws IOException {
            if (channel instanceof SeekableByteChannel seekable) {
                seekable.position(position);
                return;
            }
            if (position != openedAt) {
                throw new IOException("Channel opened at " + openedAt + " cannot seek to " + position);
            }
        }
    }

    public static final class PositionTrackingOutputStream extends FilterOutputStream {
        private long position;

        public PositionTrackingOutputStream(OutputStream outputStream, long initialPosition) {
            super(outputStream);
            position = initialPosition;
        }

        public long getPosition() {
            return position;
        }

        @Override
        public void write(int value) throws IOException {
            out.write(value);
            position++;
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            out.write(bytes, offset, length);
            position += length;
        }
    }

    public static long determineOutputPosition(OutputStream stream, long fallback) throws IOException {
        if (stream instanceof PositionableOutputStream positionable) {
            return positionable.getPosition();
        }
        if (stream instanceof FileOutputStream fileOutputStream) {
            return fileOutputStream.getChannel().position();
        }
        return fallback;
    }

    public static long determinePosition(InputStream stream, long fallback) throws IOException {
        if (stream instanceof PositionableInputStream positionable) {
            return positionable.getPosition();
        }
        if (stream instanceof FileInputStream fileInputStream) {
            return fileInputStream.getChannel().position();
        }
        return fallback;
    }

    public static String filenameOnly(String filename) {
        try {
            Path path = Path.of(filename);
            Path leaf = path.getFileName();
            return leaf == null ? filename : leaf.toString();
        } catch (Throwable ignored) {
            return filename;
        }
    }

    public static boolean isQueuedResponse(String message) {
        int end = message.length();
        while (end > 0 && message.charAt(end - 1) == '.') {
            end--;
        }
        return message.substring(0, end).equalsIgnoreCase("Queued");
    }

    public static void seekInputStream(InputStream stream, long position) throws IOException {
        if (stream instanceof PositionableInputStream positionable) {
            positionable.setPosition(position);
            return;
        }
        if (stream instanceof FileInputStream fileInputStream) {
            fileInputStream.getChannel().position(position);
            return;
        }
        if (stream instanceof ByteArrayInputStream) {
            stream.reset();
            skipFully(stream, position);
            return;
        }
        throw new IOException("Input stream is not seekable");
    }

    public static void seekOutputStream(OutputStream stream, long position) throws IOException {
        if (stream instanceof PositionableOutputStream positionable) {
            positionable.setPosition(position);
            return;
        }
        if (stream instanceof FileOutputStream fileOutputStream) {
            fileOutputStream.getChannel().position(position);
            return;
        }
        throw new IOException("Output stream is not seekable");
    }

    private static void skipFully(InputStream stream, long count) throws IOException {
        long remaining = count;
        while (remaining > 0) {
            long skipped = stream.skip(remaining);
            if (skipped <= 0) {
                if (stream.read() < 0) {
                    throw new IOException("Input stream ended before position " + count);
                }
                skipped = 1;
            }
            remaining -= skipped;
        }
    }
}
