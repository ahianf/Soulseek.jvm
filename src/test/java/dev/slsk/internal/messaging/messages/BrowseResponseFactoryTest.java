// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.exceptions.MessageCompressionException;
import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.share.BrowseResponseMessage;
import dev.slsk.internal.share.File;
import dev.slsk.internal.share.FileAttribute;
import dev.slsk.internal.share.SharedDirectory;
import dev.slsk.internal.share.WireFileAttribute;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BrowseResponseFactoryTest {
    @Test
    @DisplayName("Empty browse response round trips with compatibility fields")
    void emptyResponseRoundTrips() {
        byte[] bytes = new BrowseResponseMessage().toByteArray();
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(bytes, MessageCode.Peer.class);

        assertEquals(MessageCode.Peer.BROWSE_RESPONSE, reader.readCode());
        reader.decompress();
        assertEquals(0, reader.readInteger());
        assertEquals(0, reader.readInteger());
        assertEquals(0, reader.readInteger());
        assertEquals(0, reader.getRemaining());

        BrowseResponseMessage parsed = BrowseResponseFactory.fromByteArray(bytes);
        assertEquals(0, parsed.directoryCount());
        assertEquals(0, parsed.lockedDirectoryCount());
    }

    @Test
    @DisplayName("Complete browse response preserves all directory data")
    void completeResponseRoundTrips() {
        SharedDirectory open = new SharedDirectory(
                "open",
                List.of(new File(
                        1,
                        "one",
                        0x0102030405060708L,
                        ".mp3",
                        List.of(new FileAttribute(WireFileAttribute.BIT_RATE, 320)))));
        SharedDirectory locked = new SharedDirectory("locked", List.of(new File(2, "two", 42, ".txt")));

        BrowseResponseMessage parsed = BrowseResponseFactory.fromByteArray(
                new BrowseResponseMessage(List.of(open), List.of(locked)).toByteArray());

        assertEquals(1, parsed.directoryCount());
        assertDirectoryEquals(open, parsed.directories().getFirst());
        assertEquals(1, parsed.lockedDirectoryCount());
        assertDirectoryEquals(locked, parsed.lockedDirectories().getFirst());
    }

    @Test
    @DisplayName("Parser supports old messages without locked-file fields")
    void parserSupportsLegacyPayloads() {
        byte[] withoutCompatibilityFields = new MessageBuilder()
                .writeCode(MessageCode.Peer.BROWSE_RESPONSE)
                .writeInteger(1)
                .writeDirectory(new SharedDirectory("root"))
                .compress()
                .build();
        byte[] withUnknownOnly = new MessageBuilder()
                .writeCode(MessageCode.Peer.BROWSE_RESPONSE)
                .writeInteger(0)
                .writeInteger(99)
                .compress()
                .build();

        BrowseResponseMessage one = BrowseResponseFactory.fromByteArray(withoutCompatibilityFields);
        BrowseResponseMessage two = BrowseResponseFactory.fromByteArray(withUnknownOnly);

        assertEquals(1, one.directoryCount());
        assertEquals("root", one.directories().getFirst().name());
        assertEquals(0, one.lockedDirectoryCount());
        assertEquals(0, two.directoryCount());
        assertEquals(0, two.lockedDirectoryCount());
    }

    @Test
    @DisplayName("Parser repairs sign-extended 32-bit file sizes")
    void parserRepairsOverflowedFileSize() {
        byte[] bytes = new MessageBuilder()
                .writeCode(MessageCode.Peer.BROWSE_RESPONSE)
                .writeInteger(1)
                .writeString("root")
                .writeInteger(1)
                .writeByte(0)
                .writeString("f")
                .writeLong(-1180018327L)
                .writeString("")
                .writeInteger(0)
                .compress()
                .build();

        File file = BrowseResponseFactory.fromByteArray(bytes)
                .directories()
                .getFirst()
                .files()
                .getFirst();

        assertEquals(3114948969L, file.size());
    }

    @Test
    @DisplayName("Parser rejects mismatch compression and missing data")
    void parserRejectsInvalidMessages() {
        MessageException mismatch = assertThrows(
                MessageException.class,
                () -> BrowseResponseFactory.fromByteArray(new TransferResponse(1).toByteArray()));
        assertEquals(
                "Message Code mismatch creating BrowseResponseMessage (expected: 5, received: 41)",
                mismatch.getMessage());
        byte[] uncompressed = new MessageBuilder()
                .writeCode(MessageCode.Peer.BROWSE_RESPONSE)
                .writeBytes(new byte[] {0, 1, 2, 3})
                .build();
        assertThrows(MessageCompressionException.class, () -> BrowseResponseFactory.fromByteArray(uncompressed));
        byte[] missingFileCount = new MessageBuilder()
                .writeCode(MessageCode.Peer.BROWSE_RESPONSE)
                .writeInteger(1)
                .writeString("root")
                .compress()
                .build();
        assertThrows(MessageReadException.class, () -> BrowseResponseFactory.fromByteArray(missingFileCount));
    }

    private static void assertDirectoryEquals(SharedDirectory expected, SharedDirectory actual) {
        assertEquals(expected.name(), actual.name());
        assertEquals(expected.fileCount(), actual.fileCount());
        for (int index = 0; index < expected.fileCount(); index++) {
            File left = expected.files().get(index);
            File right = actual.files().get(index);
            assertEquals(left.code(), right.code());
            assertEquals(left.filename(), right.filename());
            assertEquals(left.size(), right.size());
            assertEquals(left.extension(), right.extension());
            assertEquals(left.attributeCount(), right.attributeCount());
            for (int attribute = 0; attribute < left.attributeCount(); attribute++) {
                assertEquals(
                        left.attributes().get(attribute).type(),
                        right.attributes().get(attribute).type());
                assertEquals(
                        left.attributes().get(attribute).value(),
                        right.attributes().get(attribute).value());
            }
        }
    }
}
