// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.exceptions.MessageCompressionException;
import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.search.SearchResponseMessage;
import dev.slsk.internal.share.File;
import dev.slsk.internal.share.FileAttribute;
import dev.slsk.internal.share.FileAttributeType;
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
        byte[] bytes = new SearchResponseMessage("alice", 0x12345678, true, 1000, 7, List.of(open), List.of(locked))
                .toByteArray();

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

        SearchResponseMessage parsed = SearchResponseFactory.fromByteArray(bytes);
        assertEquals("alice", parsed.username());
        assertEquals(0x12345678, parsed.token());
        assertTrue(parsed.hasFreeUploadSlot());
        assertEquals(1000, parsed.uploadSpeed());
        assertEquals(7, parsed.queueLength());
        assertEquals(1, parsed.fileCount());
        assertFileEquals(open, parsed.files().getFirst());
        assertEquals(1, parsed.lockedFileCount());
        assertFileEquals(locked, parsed.lockedFiles().getFirst());
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

        SearchResponseMessage parsed = SearchResponseFactory.fromByteArray(bytes);

        assertEquals("u", parsed.username());
        assertEquals(1, parsed.token());
        assertFalse(parsed.hasFreeUploadSlot());
        assertEquals(2, parsed.uploadSpeed());
        assertEquals(3, parsed.queueLength());
        assertEquals(0, parsed.fileCount());
        assertEquals(0, parsed.lockedFileCount());
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

        SearchResponseMessage parsed = SearchResponseFactory.fromByteArray(bytes);

        assertTrue(parsed.hasFreeUploadSlot());
        assertEquals(3, parsed.queueLength());
        assertEquals(1, parsed.lockedFileCount());
        assertFileEquals(locked, parsed.lockedFiles().getFirst());
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

        SearchResponseMessage parsed = SearchResponseFactory.fromByteArray(bytes);

        assertTrue(parsed.hasFreeUploadSlot());
        assertEquals(0, parsed.lockedFileCount());
    }

    @Test
    @DisplayName("Parser rejects mismatch compression and malformed data")
    void parserRejectsInvalidMessages() {
        assertThrows(
                MessageException.class,
                () -> SearchResponseFactory.fromByteArray(new BrowseRequestMessage().toByteArray()));
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
        assertEquals(expected.code(), actual.code());
        assertEquals(expected.filename(), actual.filename());
        assertEquals(expected.size(), actual.size());
        assertEquals(expected.extension(), actual.extension());
        assertEquals(expected.attributeCount(), actual.attributeCount());
        for (int index = 0; index < expected.attributeCount(); index++) {
            assertEquals(
                    expected.attributes().get(index).type(),
                    actual.attributes().get(index).type());
            assertEquals(
                    expected.attributes().get(index).value(),
                    actual.attributes().get(index).value());
        }
    }
}
