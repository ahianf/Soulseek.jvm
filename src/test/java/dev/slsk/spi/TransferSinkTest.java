// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.spi;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The guarantee the file sink exists to make: nothing incomplete is ever visible
 * at the destination.
 *
 * <p>Every client gets this wrong the same way — writing straight to the
 * destination, so an interrupted download leaves a file that looks finished and
 * plays for thirty seconds. Moving the decision into the library is most of the
 * reason the SPI is shaped as three calls rather than "give me a stream".
 */
class TransferSinkTest {

    private static void write(WritableByteChannel channel, String text) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static Path partOf(Path destination) {
        return destination.resolveSibling(destination.getFileName() + ".part");
    }

    @Test
    @DisplayName("nothing appears at the destination until commit")
    void theDestinationStaysAbsentWhileWriting(@TempDir Path directory) throws IOException {
        Path destination = directory.resolve("song.mp3");
        TransferSink sink = TransferSink.file(destination);

        write(sink.open(0), "half");
        assertFalse(Files.exists(destination), "a download in flight must not be visible");
        assertTrue(Files.exists(partOf(destination)));

        sink.commit();
        assertEquals("half", Files.readString(destination));
        assertFalse(Files.exists(partOf(destination)), "the part file is consumed by the commit");
    }

    @Test
    @DisplayName("an interrupted download leaves no partial file at the destination")
    void discardNeverPublishesAPartialFile(@TempDir Path directory) throws IOException {
        Path destination = directory.resolve("song.mp3");
        TransferSink sink = TransferSink.file(destination);

        write(sink.open(0), "partial");
        sink.discard();

        assertFalse(Files.exists(destination));
        assertEquals("partial", Files.readString(partOf(destination)), "the bytes survive for a retry");
    }

    @Test
    @DisplayName("a resume starts where the last attempt stopped")
    void resumeAppendsAtTheOffset(@TempDir Path directory) throws IOException {
        Path destination = directory.resolve("song.mp3");

        TransferSink first = TransferSink.file(destination);
        write(first.open(0), "abcde");
        first.discard();

        TransferSink second = TransferSink.file(destination);
        write(second.open(5), "fghij");
        second.commit();

        assertEquals("abcdefghij", Files.readString(destination));
    }

    /**
     * A resume is told where the peer will start sending, and the bytes already
     * past that point came from an attempt that did not finish. They are not
     * known to be the same bytes, so they go.
     */
    @Test
    @DisplayName("a resume discards anything past the offset rather than trusting it")
    void resumeTruncatesTheTail(@TempDir Path directory) throws IOException {
        Path destination = directory.resolve("song.mp3");

        TransferSink first = TransferSink.file(destination);
        write(first.open(0), "abcdeXXXXX");
        first.discard();

        TransferSink second = TransferSink.file(destination);
        write(second.open(5), "fghij");
        second.commit();

        assertEquals("abcdefghij", Files.readString(destination));
    }

    @Test
    @DisplayName("committing over an existing file replaces it whole")
    void commitReplacesAnExistingFile(@TempDir Path directory) throws IOException {
        Path destination = directory.resolve("song.mp3");
        Files.writeString(destination, "the older, longer copy");

        TransferSink sink = TransferSink.file(destination);
        write(sink.open(0), "new");
        sink.commit();

        assertArrayEquals("new".getBytes(StandardCharsets.UTF_8), Files.readAllBytes(destination));
    }

    @Test
    void createsTheDirectoriesTheDestinationNeeds(@TempDir Path directory) throws IOException {
        Path destination = directory.resolve("artist").resolve("album").resolve("song.mp3");

        TransferSink sink = TransferSink.file(destination);
        write(sink.open(0), "bytes");
        sink.commit();

        assertEquals("bytes", Files.readString(destination));
    }

    @Test
    void rejectsANegativeOffset(@TempDir Path directory) {
        TransferSink sink = TransferSink.file(directory.resolve("song.mp3"));
        assertThrows(IllegalArgumentException.class, () -> sink.open(-1));
    }

    @Test
    @DisplayName("discard does not throw, because it is called from a path that already failed")
    void discardIsSafeEvenWithoutAnOpen(@TempDir Path directory) {
        TransferSink.file(directory.resolve("song.mp3")).discard();
    }
}
