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
