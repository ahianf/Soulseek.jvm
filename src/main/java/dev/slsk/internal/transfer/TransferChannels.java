// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

import dev.slsk.exceptions.TransferStreamException;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.LongFunction;
import java.util.function.Supplier;

/** Channel adapters, ownership metadata, and position tracking for transfers. */
public final class TransferChannels {

    private TransferChannels() {}

    /** Opens a download destination for the negotiated offset. */
    @FunctionalInterface
    public interface DestinationFactory {
        TrackingWritableChannel open(long offset, boolean positionAutomatically);
    }

    /** Opens an upload source for the negotiated offset. */
    @FunctionalInterface
    public interface SourceFactory {
        TrackingReadableChannel open(long offset, boolean positionAutomatically);
    }

    /** Adapts the legacy stream edge to a channel before it enters a transfer run. */
    public static DestinationFactory destination(Supplier<OutputStream> factory) {
        Objects.requireNonNull(factory, "outputStreamFactory");
        return (offset, positionAutomatically) -> writable(
                Objects.requireNonNull(factory.get(), "outputStreamFactory result"), offset, positionAutomatically);
    }

    /** Uses a channel factory whose contract already positions it at the requested offset. */
    public static DestinationFactory destination(LongFunction<? extends WritableByteChannel> factory) {
        Objects.requireNonNull(factory, "outputChannelFactory");
        return (offset, ignored) ->
                trackWritable(Objects.requireNonNull(factory.apply(offset), "outputChannelFactory result"), offset);
    }

    /** Adapts the legacy stream edge to a channel before it enters a transfer run. */
    public static SourceFactory source(LongFunction<InputStream> factory) {
        Objects.requireNonNull(factory, "inputStreamFactory");
        return (offset, positionAutomatically) -> readable(
                Objects.requireNonNull(factory.apply(offset), "inputStreamFactory result"),
                offset,
                positionAutomatically);
    }

    /** Uses a channel factory whose contract already positions it at the requested offset. */
    public static SourceFactory sourceChannel(LongFunction<? extends ReadableByteChannel> factory) {
        Objects.requireNonNull(factory, "inputChannelFactory");
        return (offset, ignored) ->
                trackReadable(Objects.requireNonNull(factory.apply(offset), "inputChannelFactory result"), offset);
    }

    /** Wraps a destination channel that is already positioned. */
    public static TrackingWritableChannel trackWritable(WritableByteChannel channel, long position) {
        return new TrackingWritableChannel(channel, position, null);
    }

    /** Wraps a source channel that is already positioned. */
    public static TrackingReadableChannel trackReadable(ReadableByteChannel channel, long position) {
        return new TrackingReadableChannel(channel, position);
    }

    private static TrackingWritableChannel writable(OutputStream stream, long offset, boolean positionAutomatically) {
        try {
            WritableByteChannel channel;
            long initialPosition;
            if (stream instanceof FileOutputStream file) {
                channel = file.getChannel();
                if (positionAutomatically && offset > 0) {
                    file.getChannel().position(offset);
                }
                initialPosition = file.getChannel().position();
            } else {
                if (positionAutomatically && offset > 0) {
                    throw new IOException("Output stream is not seekable");
                }
                channel = Channels.newChannel(stream);
                initialPosition = 0;
            }
            return new TrackingWritableChannel(channel, initialPosition, stream);
        } catch (IOException failure) {
            throw new TransferStreamException(
                    "Requested non-zero start offset but output stream does not support seeking", failure);
        }
    }

    private static TrackingReadableChannel readable(InputStream stream, long offset, boolean positionAutomatically) {
        try {
            ReadableByteChannel channel;
            long initialPosition;
            if (stream instanceof FileInputStream file) {
                channel = file.getChannel();
                if (positionAutomatically && offset > 0) {
                    file.getChannel().position(offset);
                }
                initialPosition = file.getChannel().position();
            } else if (stream instanceof ByteArrayInputStream && positionAutomatically && offset > 0) {
                stream.reset();
                skipFully(stream, offset);
                channel = Channels.newChannel(stream);
                initialPosition = offset;
            } else {
                if (positionAutomatically && offset > 0) {
                    throw new IOException("Input stream is not seekable");
                }
                channel = Channels.newChannel(stream);
                // With automatic seeking disabled the stream position is the
                // caller's responsibility, while transfer progress still
                // starts at the peer-negotiated offset.
                initialPosition = offset;
            }
            return new TrackingReadableChannel(channel, initialPosition);
        } catch (IOException failure) {
            throw new TransferStreamException(
                    "Requested non-zero start offset but input stream does not support seeking", failure);
        }
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

    /** A readable channel that reports the absolute transfer position. */
    public static final class TrackingReadableChannel implements ReadableByteChannel {
        private final ReadableByteChannel channel;
        private long position;

        private TrackingReadableChannel(ReadableByteChannel channel, long position) {
            this.channel = Objects.requireNonNull(channel, "channel");
            this.position = position;
        }

        public long position() {
            return position;
        }

        @Override
        public int read(ByteBuffer destination) throws IOException {
            int read = channel.read(destination);
            if (read > 0) {
                position += read;
            }
            return read;
        }

        @Override
        public boolean isOpen() {
            return channel.isOpen();
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    /** A writable channel that reports position and retains stream flush semantics. */
    public static final class TrackingWritableChannel implements WritableByteChannel {
        private final WritableByteChannel channel;
        private final Flushable flushable;
        private long position;

        private TrackingWritableChannel(WritableByteChannel channel, long position, Flushable flushable) {
            this.channel = Objects.requireNonNull(channel, "channel");
            this.position = position;
            this.flushable = flushable;
        }

        public long position() {
            return position;
        }

        public void flush() throws IOException {
            if (flushable != null) {
                flushable.flush();
            }
        }

        @Override
        public int write(ByteBuffer source) throws IOException {
            int written = channel.write(source);
            if (written > 0) {
                position += written;
            }
            return written;
        }

        @Override
        public boolean isOpen() {
            return channel.isOpen();
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }
}
