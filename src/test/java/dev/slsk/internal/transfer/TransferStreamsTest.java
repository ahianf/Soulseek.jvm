// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

import static dev.slsk.internal.transfer.TransferStreams.determinePosition;
import static dev.slsk.internal.transfer.TransferStreams.positionedStream;
import static dev.slsk.internal.transfer.TransferStreams.seekInputStream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The stream a served share is read through, and whether it can be resumed.
 *
 * <p>These exist because the suite tested seeking with {@link
 * java.io.ByteArrayInputStream} — which {@link TransferStreams#seekInputStream}
 * has always handled — while every real upload went out over a channel, which
 * it did not. So a resumed upload failed on the network in a case no test could
 * reach, and the fifteen files starting at zero passed either way. What is
 * pinned here is the type production actually uses.
 */
class TransferStreamsTest {

    private static Path file(Path directory, int size) throws IOException {
        byte[] bytes = new byte[size];
        for (int index = 0; index < size; index++) {
            bytes[index] = (byte) index;
        }
        Path path = directory.resolve("share.bin");
        Files.write(path, bytes);
        return path;
    }

    @Nested
    @DisplayName("a channel-backed stream")
    class ChannelBacked {

        @Test
        @DisplayName("is seekable, which Channels.newInputStream alone is not")
        void isSeekable(@TempDir Path directory) throws IOException {
            Path path = file(directory, 512);

            try (FileChannel bare = FileChannel.open(path, StandardOpenOption.READ)) {
                InputStream plain = Channels.newInputStream(bare);
                // The defect, pinned: this is what serve() used to hand to an
                // upload, and a resume asked to seek it got this.
                assertThrows(IOException.class, () -> seekInputStream(plain, 300));
            }

            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                channel.position(300);
                InputStream stream = positionedStream(channel, 300);
                seekInputStream(stream, 300);
                assertEquals(300, determinePosition(stream, -1));
                assertEquals((byte) 300, (byte) stream.read());
            }
        }

        @Test
        @DisplayName("reports the offset it was opened at, before any read")
        void reportsOpenOffset(@TempDir Path directory) throws IOException {
            Path path = file(directory, 512);
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                channel.position(128);
                assertEquals(128, determinePosition(positionedStream(channel, 128), -1));
            }
        }

        @Test
        @DisplayName("tracks the channel as bytes are read")
        void tracksReads(@TempDir Path directory) throws IOException {
            Path path = file(directory, 512);
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                channel.position(100);
                InputStream stream = positionedStream(channel, 100);
                assertEquals(10, stream.readNBytes(10).length);
                assertEquals(110, determinePosition(stream, -1));
            }
        }

        @Test
        @DisplayName("reads from the offset, not from zero")
        void readsFromOffset(@TempDir Path directory) throws IOException {
            Path path = file(directory, 512);
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                channel.position(400);
                byte[] tail = positionedStream(channel, 400).readAllBytes();
                assertEquals(112, tail.length);
                assertEquals((byte) 400, tail[0]);
            }
        }
    }

    @Nested
    @DisplayName("a channel that cannot seek")
    class NotSeekable {

        /** A share catalog is free to return one of these; only reads are promised. */
        private ReadableByteChannel forwardOnly(byte[] bytes) {
            return Channels.newChannel(new java.io.ByteArrayInputStream(bytes));
        }

        @Test
        @DisplayName("still answers where it was opened")
        void answersOpenOffset() throws IOException {
            InputStream stream = positionedStream(forwardOnly(new byte[8]), 64);
            assertEquals(64, determinePosition(stream, -1));
            // The offset it was opened at is a position it is already at.
            seekInputStream(stream, 64);
        }

        @Test
        @DisplayName("refuses a seek it cannot honour rather than sending the wrong bytes")
        void refusesRealSeek() {
            InputStream stream = positionedStream(forwardOnly(new byte[8]), 64);
            assertThrows(IOException.class, () -> seekInputStream(stream, 96));
        }
    }

    @Nested
    @DisplayName("closing")
    class Closing {

        @Test
        @DisplayName("closes the channel underneath")
        void closesChannel(@TempDir Path directory) throws IOException {
            Path path = file(directory, 32);
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
            positionedStream(channel, 0).close();
            assertThrows(IOException.class, () -> channel.read(ByteBuffer.allocate(1)));
        }
    }
}
