// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.exceptions.MessageCompressionException;
import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.internal.BrowseResponse;
import dev.slsk.internal.Directory;
import dev.slsk.internal.File;
import dev.slsk.internal.FileAttribute;
import dev.slsk.internal.FileAttributeType;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BrowseResponseFactoryTest {
    @Test
    @DisplayName("Empty browse response round trips with compatibility fields")
    void emptyResponseRoundTrips() {
        byte[] bytes = new BrowseResponse().toByteArray();
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(bytes, MessageCode.Peer.class);

        assertEquals(MessageCode.Peer.BROWSE_RESPONSE, reader.readCode());
        reader.decompress();
        assertEquals(0, reader.readInteger());
        assertEquals(0, reader.readInteger());
        assertEquals(0, reader.readInteger());
        assertEquals(0, reader.getRemaining());

        BrowseResponse parsed = BrowseResponseFactory.fromByteArray(bytes);
        assertEquals(0, parsed.getDirectoryCount());
        assertEquals(0, parsed.getLockedDirectoryCount());
    }

    @Test
    @DisplayName("Complete browse response preserves all directory data")
    void completeResponseRoundTrips() {
        Directory open = new Directory(
                "open",
                List.of(new File(
                        1,
                        "one",
                        0x0102030405060708L,
                        ".mp3",
                        List.of(new FileAttribute(FileAttributeType.BIT_RATE, 320)))));
        Directory locked = new Directory("locked", List.of(new File(2, "two", 42, ".txt")));

        BrowseResponse parsed =
                BrowseResponseFactory.fromByteArray(new BrowseResponse(List.of(open), List.of(locked)).toByteArray());

        assertEquals(1, parsed.getDirectoryCount());
        assertDirectoryEquals(open, parsed.getDirectories().getFirst());
        assertEquals(1, parsed.getLockedDirectoryCount());
        assertDirectoryEquals(locked, parsed.getLockedDirectories().getFirst());
    }

    @Test
    @DisplayName("Parser supports old messages without locked-file fields")
    void parserSupportsLegacyPayloads() {
        byte[] withoutCompatibilityFields = new MessageBuilder()
                .writeCode(MessageCode.Peer.BROWSE_RESPONSE)
                .writeInteger(1)
                .writeDirectory(new Directory("root"))
                .compress()
                .build();
        byte[] withUnknownOnly = new MessageBuilder()
                .writeCode(MessageCode.Peer.BROWSE_RESPONSE)
                .writeInteger(0)
                .writeInteger(99)
                .compress()
                .build();

        BrowseResponse one = BrowseResponseFactory.fromByteArray(withoutCompatibilityFields);
        BrowseResponse two = BrowseResponseFactory.fromByteArray(withUnknownOnly);

        assertEquals(1, one.getDirectoryCount());
        assertEquals("root", one.getDirectories().getFirst().getName());
        assertEquals(0, one.getLockedDirectoryCount());
        assertEquals(0, two.getDirectoryCount());
        assertEquals(0, two.getLockedDirectoryCount());
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
                .getDirectories()
                .getFirst()
                .getFiles()
                .getFirst();

        assertEquals(3114948969L, file.getSize());
    }

    @Test
    @DisplayName("Parser rejects mismatch compression and missing data")
    void parserRejectsInvalidMessages() {
        assertThrows(
                MessageException.class,
                () -> BrowseResponseFactory.fromByteArray(new TransferResponse(1).toByteArray()));
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

    private static void assertDirectoryEquals(Directory expected, Directory actual) {
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getFileCount(), actual.getFileCount());
        for (int index = 0; index < expected.getFileCount(); index++) {
            File left = expected.getFiles().get(index);
            File right = actual.getFiles().get(index);
            assertEquals(left.getCode(), right.getCode());
            assertEquals(left.getFilename(), right.getFilename());
            assertEquals(left.getSize(), right.getSize());
            assertEquals(left.getExtension(), right.getExtension());
            assertEquals(left.getAttributeCount(), right.getAttributeCount());
            for (int attribute = 0; attribute < left.getAttributeCount(); attribute++) {
                assertEquals(
                        left.getAttributes().get(attribute).getType(),
                        right.getAttributes().get(attribute).getType());
                assertEquals(
                        left.getAttributes().get(attribute).getValue(),
                        right.getAttributes().get(attribute).getValue());
            }
        }
    }
}
