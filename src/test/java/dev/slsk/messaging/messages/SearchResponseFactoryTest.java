// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.File;
import dev.slsk.FileAttribute;
import dev.slsk.FileAttributeType;
import dev.slsk.SearchResponse;
import dev.slsk.exceptions.MessageCompressionException;
import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SearchResponseFactoryTest {
    @Test
    @DisplayName("Complete search response round trips all wire fields")
    void completeResponseRoundTrips() {
        File open = new File(
                2,
                "open",
                0x0102030405060708L,
                ".mp3",
                List.of(
                        new FileAttribute(FileAttributeType.BIT_DEPTH, 24),
                        new FileAttribute(FileAttributeType.BIT_RATE, 320)));
        File locked = new File(3, "locked", 42, ".txt");
        byte[] bytes =
                new SearchResponse("alice", 0x12345678, true, 1000, 7, List.of(open), List.of(locked)).toByteArray();

        MessageReader<MessageCode.Peer> reader = new MessageReader<>(bytes, MessageCode.Peer.class);
        assertEquals(MessageCode.Peer.SEARCH_RESPONSE, reader.readCode());
        reader.decompress();
        assertEquals("alice", reader.readString());
        assertEquals(0x12345678, reader.readInteger());
        assertEquals(1, reader.readInteger());
        assertFileEquals(open, reader.readFile());
        assertEquals(1, reader.readByte());
        assertEquals(1000, reader.readInteger());
        assertEquals(7, reader.readInteger());
        assertEquals(0, reader.readInteger());
        assertEquals(1, reader.readInteger());
        assertFileEquals(locked, reader.readFile());
        assertEquals(0, reader.getRemaining());

        SearchResponse parsed = SearchResponseFactory.fromByteArray(bytes);
        assertEquals("alice", parsed.getUsername());
        assertEquals(0x12345678, parsed.getToken());
        assertTrue(parsed.hasFreeUploadSlot());
        assertEquals(1000, parsed.getUploadSpeed());
        assertEquals(7, parsed.getQueueLength());
        assertEquals(1, parsed.getFileCount());
        assertFileEquals(open, parsed.getFiles().getFirst());
        assertEquals(1, parsed.getLockedFileCount());
        assertFileEquals(locked, parsed.getLockedFiles().getFirst());
    }

    @Test
    @DisplayName("Parser supports legacy response without compatibility fields")
    void parserSupportsLegacyResponse() {
        byte[] bytes = new MessageBuilder()
                .writeCode(MessageCode.Peer.SEARCH_RESPONSE)
                .writeString("u")
                .writeInteger(1)
                .writeInteger(0)
                .writeByte(0)
                .writeInteger(2)
                .writeInteger(3)
                .compress()
                .build();

        SearchResponse parsed = SearchResponseFactory.fromByteArray(bytes);

        assertEquals("u", parsed.getUsername());
        assertEquals(1, parsed.getToken());
        assertFalse(parsed.hasFreeUploadSlot());
        assertEquals(2, parsed.getUploadSpeed());
        assertEquals(3, parsed.getQueueLength());
        assertEquals(0, parsed.getFileCount());
        assertEquals(0, parsed.getLockedFileCount());
    }

    @Test
    @DisplayName("Parser supports eight-byte queue field used by newer peers")
    void parserSupportsEightByteQueueField() {
        File locked = new File(1, "f", 2, "e");
        byte[] bytes = new MessageBuilder()
                .writeCode(MessageCode.Peer.SEARCH_RESPONSE)
                .writeString("u")
                .writeInteger(1)
                .writeInteger(0)
                .writeByte(255)
                .writeInteger(2)
                .writeLong(3)
                .writeInteger(1)
                .writeFile(locked)
                .compress()
                .build();

        SearchResponse parsed = SearchResponseFactory.fromByteArray(bytes);

        assertTrue(parsed.hasFreeUploadSlot());
        assertEquals(3, parsed.getQueueLength());
        assertEquals(1, parsed.getLockedFileCount());
        assertFileEquals(locked, parsed.getLockedFiles().getFirst());
    }

    @Test
    @DisplayName("Parser ignores unknown compatibility integer")
    void parserIgnoresUnknownInteger() {
        byte[] bytes = new MessageBuilder()
                .writeCode(MessageCode.Peer.SEARCH_RESPONSE)
                .writeString("u")
                .writeInteger(1)
                .writeInteger(0)
                .writeByte(1)
                .writeInteger(2)
                .writeInteger(3)
                .writeInteger(-1)
                .writeInteger(0)
                .compress()
                .build();

        SearchResponse parsed = SearchResponseFactory.fromByteArray(bytes);

        assertTrue(parsed.hasFreeUploadSlot());
        assertEquals(0, parsed.getLockedFileCount());
    }

    @Test
    @DisplayName("Parser rejects mismatch compression and malformed data")
    void parserRejectsInvalidMessages() {
        assertThrows(
                MessageException.class, () -> SearchResponseFactory.fromByteArray(new BrowseRequest().toByteArray()));
        byte[] uncompressed = new MessageBuilder()
                .writeCode(MessageCode.Peer.SEARCH_RESPONSE)
                .writeBytes(new byte[] {0, 1, 2, 3})
                .build();
        assertThrows(MessageCompressionException.class, () -> SearchResponseFactory.fromByteArray(uncompressed));
        byte[] missing = new MessageBuilder()
                .writeCode(MessageCode.Peer.SEARCH_RESPONSE)
                .writeString("u")
                .compress()
                .build();
        assertThrows(MessageReadException.class, () -> SearchResponseFactory.fromByteArray(missing));
        byte[] countMismatch = new MessageBuilder()
                .writeCode(MessageCode.Peer.SEARCH_RESPONSE)
                .writeString("u")
                .writeInteger(1)
                .writeInteger(2)
                .writeFile(new File(1, "f", 2, "e"))
                .writeByte(0)
                .writeInteger(0)
                .writeInteger(0)
                .compress()
                .build();
        assertThrows(MessageReadException.class, () -> SearchResponseFactory.fromByteArray(countMismatch));
    }

    @Test
    @DisplayName("Factory rejects null response serialization")
    void factoryRejectsNullSerialization() {
        assertThrows(NullPointerException.class, () -> SearchResponseFactory.toByteArray(null));
    }

    private static void assertFileEquals(File expected, File actual) {
        assertEquals(expected.getCode(), actual.getCode());
        assertEquals(expected.getFilename(), actual.getFilename());
        assertEquals(expected.getSize(), actual.getSize());
        assertEquals(expected.getExtension(), actual.getExtension());
        assertEquals(expected.getAttributeCount(), actual.getAttributeCount());
        for (int index = 0; index < expected.getAttributeCount(); index++) {
            assertEquals(
                    expected.getAttributes().get(index).getType(),
                    actual.getAttributes().get(index).getType());
            assertEquals(
                    expected.getAttributes().get(index).getValue(),
                    actual.getAttributes().get(index).getValue());
        }
    }
}
