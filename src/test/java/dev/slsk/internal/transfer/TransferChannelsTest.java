// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.exceptions.TransferStreamException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TransferChannelsTest {

    @Test
    void tracksAChannelOpenedAtAnOffset(@TempDir Path directory) throws IOException {
        Path file = directory.resolve("share.bin");
        Files.write(file, new byte[] {0, 1, 2, 3, 4});
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            channel.position(2);
            TransferChannels.TrackingReadableChannel source =
                    TransferChannels.sourceChannel(offset -> channel).open(2, true);

            ByteBuffer bytes = ByteBuffer.allocate(2);
            assertEquals(2, source.position());
            assertEquals(2, source.read(bytes));
            assertEquals(4, source.position());
            assertArrayEquals(new byte[] {2, 3}, bytes.array());
        }
    }

    @Test
    void tracksAForwardOnlyChannelFromItsDeclaredOffset() throws IOException {
        TransferChannels.TrackingReadableChannel source = TransferChannels.sourceChannel(
                        offset -> Channels.newChannel(new ByteArrayInputStream(new byte[] {7, 8})))
                .open(64, true);

        assertEquals(64, source.position());
        assertEquals(2, source.read(ByteBuffer.allocate(2)));
        assertEquals(66, source.position());
    }

    @Test
    void streamEdgeSeeksByteArraysWhenRequested() throws IOException {
        TransferChannels.TrackingReadableChannel source = TransferChannels.source(
                        offset -> new ByteArrayInputStream(new byte[] {0, 1, 2, 3}))
                .open(2, true);
        ByteBuffer byteValue = ByteBuffer.allocate(1);

        assertEquals(2, source.position());
        assertEquals(1, source.read(byteValue));
        assertEquals(2, byteValue.array()[0]);
    }

    @Test
    void streamEdgeRejectsAnUnsupportedResumeSeek() {
        InputStream forwardOnly = new InputStream() {
            @Override
            public int read() {
                return -1;
            }
        };

        assertThrows(
                TransferStreamException.class,
                () -> TransferChannels.source(offset -> forwardOnly).open(1, true));
    }

    @Test
    void outputStreamEdgeTracksFlushesAndRetainsOwnership() throws IOException {
        AtomicBoolean flushed = new AtomicBoolean();
        ByteArrayOutputStream output = new ByteArrayOutputStream() {
            @Override
            public void flush() throws IOException {
                flushed.set(true);
                super.flush();
            }
        };
        TransferChannels.TrackingWritableChannel destination =
                TransferChannels.destination(() -> output).open(0, true);

        destination.write(ByteBuffer.wrap(new byte[] {1, 2, 3}));
        destination.flush();

        assertEquals(3, destination.position());
        assertArrayEquals(new byte[] {1, 2, 3}, output.toByteArray());
        assertTrue(flushed.get());
        assertTrue(destination.isOpen());
    }

    @Test
    void closingTheTrackingChannelClosesItsDelegate(@TempDir Path directory) throws IOException {
        Path file = directory.resolve("share.bin");
        Files.write(file, new byte[] {1});
        FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
        TransferChannels.TrackingReadableChannel source = TransferChannels.trackReadable(channel, 0);

        source.close();

        assertFalse(channel.isOpen());
        assertThrows(IOException.class, () -> channel.read(ByteBuffer.allocate(1)));
    }
}
