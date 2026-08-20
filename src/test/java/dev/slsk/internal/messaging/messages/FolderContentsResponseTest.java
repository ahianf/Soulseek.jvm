// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.exceptions.MessageCompressionException;
import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.share.Directory;
import dev.slsk.internal.share.File;
import dev.slsk.internal.share.FileAttribute;
import dev.slsk.internal.share.FileAttributeType;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FolderContentsResponseTest {
    @Test
    @DisplayName("Constructor snapshots directory data")
    void constructorSnapshotsDirectoryData() {
        Directory directory = new Directory("root");
        List<Directory> source = new ArrayList<>();
        source.add(directory);

        FolderContentsResponse response = new FolderContentsResponse(17, "root", source);
        source.clear();

        assertEquals(17, response.token());
        assertEquals("root", response.directoryName());
        assertEquals(1, response.directoryCount());
        assertEquals(1, response.directories().size());
        assertSame(directory, response.directories().getFirst());
        assertThrows(
                UnsupportedOperationException.class,
                () -> response.directories().clear());
        assertThrows(NullPointerException.class, () -> new FolderContentsResponse(1, "root", null));
    }

    @Test
    @DisplayName("Empty directory response round trips through compression")
    void emptyDirectoryResponseRoundTrips() {
        FolderContentsResponse outgoing =
                new FolderContentsResponse(0x12345678, "root", List.of(new Directory("root")));

        FolderContentsResponse parsed = FolderContentsResponse.fromByteArray(outgoing.toByteArray());

        assertEquals(0x12345678, parsed.token());
        assertEquals("root", parsed.directoryName());
        assertEquals(1, parsed.directoryCount());
        assertEquals("root", parsed.directories().getFirst().name());
        assertEquals(0, parsed.directories().getFirst().fileCount());
    }

    @Test
    @DisplayName("Complete response preserves directories files and attributes")
    void completeResponseRoundTrips() {
        Directory root = new Directory(
                "root",
                List.of(
                        new File(
                                1,
                                "one",
                                0x0102030405060708L,
                                ".mp3",
                                List.of(
                                        new FileAttribute(FileAttributeType.BIT_DEPTH, 24),
                                        new FileAttribute(FileAttributeType.BIT_RATE, 320))),
                        new File(2, "two", 12, ".txt")));
        Directory child = new Directory("root/child", List.of(new File(0, "three", 42, "")));

        FolderContentsResponse parsed = FolderContentsResponse.fromByteArray(
                new FolderContentsResponse(-17, "root", List.of(root, child)).toByteArray());

        assertEquals(-17, parsed.token());
        assertEquals("root", parsed.directoryName());
        assertEquals(2, parsed.directoryCount());
        assertDirectoryEquals(root, parsed.directories().get(0));
        assertDirectoryEquals(child, parsed.directories().get(1));
    }

    @Test
    @DisplayName("Serialized response has the source decompressed payload")
    void serializedResponseHasSourcePayload() {
        FolderContentsResponse response = new FolderContentsResponse(
                0x12345678,
                "r",
                List.of(new Directory(
                        "r",
                        List.of(new File(1, "f", 2, "e", List.of(new FileAttribute(FileAttributeType.BIT_RATE, 3)))))));
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(response.toByteArray(), MessageCode.Peer.class);

        assertEquals(MessageCode.Peer.FOLDER_CONTENTS_RESPONSE, reader.readCode());
        reader.decompress();
        assertEquals(0x12345678, reader.readInteger());
        assertEquals("r", reader.readString());
        assertEquals(1, reader.readInteger());
        assertEquals("r", reader.readString());
        assertEquals(1, reader.readInteger());
        assertEquals(1, reader.readByte());
        assertEquals("f", reader.readString());
        assertEquals(2, reader.readLong());
        assertEquals("e", reader.readString());
        assertEquals(1, reader.readInteger());
        assertEquals(FileAttributeType.BIT_RATE.getValue(), reader.readInteger());
        assertEquals(3, reader.readInteger());
        assertEquals(0, reader.getRemaining());
    }

    @Test
    @DisplayName("Parser rejects mismatched and uncompressed messages")
    void parserRejectsMismatchAndUncompressedData() {
        assertThrows(
                MessageException.class,
                () -> FolderContentsResponse.fromByteArray(new TransferResponse(1).toByteArray()));

        byte[] uncompressed = new MessageBuilder()
                .writeCode(MessageCode.Peer.FOLDER_CONTENTS_RESPONSE)
                .writeBytes(new byte[] {0, 1, 2, 3})
                .build();
        assertThrows(MessageCompressionException.class, () -> FolderContentsResponse.fromByteArray(uncompressed));
    }

    @Test
    @DisplayName("Parser preserves missing directory data failure")
    void parserPreservesMissingDirectoryDataFailure() {
        byte[] missingFileCount = new MessageBuilder()
                .writeCode(MessageCode.Peer.FOLDER_CONTENTS_RESPONSE)
                .writeInteger(1)
                .writeString("root")
                .writeInteger(1)
                .writeString("root")
                .compress()
                .build();

        assertThrows(MessageReadException.class, () -> FolderContentsResponse.fromByteArray(missingFileCount));
    }

    private static void assertDirectoryEquals(Directory expected, Directory actual) {
        assertEquals(expected.name(), actual.name());
        assertEquals(expected.fileCount(), actual.fileCount());
        for (int index = 0; index < expected.fileCount(); index++) {
            File expectedFile = expected.files().get(index);
            File actualFile = actual.files().get(index);
            assertEquals(expectedFile.code(), actualFile.code());
            assertEquals(expectedFile.filename(), actualFile.filename());
            assertEquals(expectedFile.size(), actualFile.size());
            assertEquals(expectedFile.extension(), actualFile.extension());
            assertEquals(expectedFile.attributeCount(), actualFile.attributeCount());
            for (int attribute = 0; attribute < expectedFile.attributeCount(); attribute++) {
                assertEquals(
                        expectedFile.attributes().get(attribute).type(),
                        actualFile.attributes().get(attribute).type());
                assertEquals(
                        expectedFile.attributes().get(attribute).value(),
                        actualFile.attributes().get(attribute).value());
            }
        }
    }
}
