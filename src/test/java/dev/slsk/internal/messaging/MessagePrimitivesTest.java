// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.exceptions.MessageCompressionException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.internal.CharacterEncoding;
import dev.slsk.internal.Directory;
import dev.slsk.internal.File;
import dev.slsk.internal.FileAttribute;
import dev.slsk.internal.FileAttributeType;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MessagePrimitivesTest {
    @Test
    @DisplayName("Builder emits the fixed little-endian golden vector")
    void builderEmitsGoldenVector() {
        byte[] message = new MessageBuilder()
                .writeCode(MessageCode.Peer.BROWSE_REQUEST)
                .writeByte(0xff)
                .writeInteger(0x1234_5678)
                .writeLong(0x0102_0304_0506_0708L)
                .writeString("Aǔ")
                .build();

        assertArrayEquals(
                new byte[] {
                    0x18,
                    0x00,
                    0x00,
                    0x00,
                    0x04,
                    0x00,
                    0x00,
                    0x00,
                    (byte) 0xff,
                    0x78,
                    0x56,
                    0x34,
                    0x12,
                    0x08,
                    0x07,
                    0x06,
                    0x05,
                    0x04,
                    0x03,
                    0x02,
                    0x01,
                    0x03,
                    0x00,
                    0x00,
                    0x00,
                    0x41,
                    (byte) 0xc7,
                    (byte) 0x94
                },
                message);
    }

    @Test
    @DisplayName("One-byte message code uses the correct frame length")
    void oneByteCodeUsesCorrectFrameLength() {
        byte[] message = new MessageBuilder()
                .writeCode(MessageCode.Distributed.SEARCH_REQUEST)
                .writeInteger(42)
                .build();

        assertArrayEquals(new byte[] {5, 0, 0, 0, 3, 42, 0, 0, 0}, message);
        MessageReader<MessageCode.Distributed> reader = new MessageReader<>(message, MessageCode.Distributed.class);
        assertEquals(MessageCode.Distributed.SEARCH_REQUEST, reader.readCode());
        assertEquals(42, reader.readInteger());
    }

    @Test
    @DisplayName("Build requires a code and code replacement retains payload")
    void buildRequiresCodeAndReplacementRetainsPayload() {
        MessageBuilder builder = new MessageBuilder().writeInteger(42);
        assertThrows(IllegalStateException.class, builder::build);

        byte[] message = builder.writeCode(MessageCode.Peer.BROWSE_REQUEST)
                .writeCode(MessageCode.Peer.INFO_REQUEST)
                .build();
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(message, MessageCode.Peer.class);
        assertEquals(MessageCode.Peer.INFO_REQUEST, reader.readCode());
        assertEquals(42, reader.readInteger());
    }

    @Test
    @DisplayName("WriteBytes appends and rejects null or post-compression writes")
    void writeBytesValidatesState() {
        MessageBuilder builder = new MessageBuilder()
                .writeCode(MessageCode.Peer.INFO_REQUEST)
                .writeBytes(new byte[] {1, 2})
                .writeBytes(new byte[] {3});

        MessageReader<MessageCode.Peer> reader = new MessageReader<>(builder.build(), MessageCode.Peer.class);
        assertArrayEquals(new byte[] {1, 2, 3}, reader.getPayload());
        assertThrows(NullPointerException.class, () -> new MessageBuilder().writeBytes(null));
        builder.compress();
        assertThrows(IllegalStateException.class, () -> builder.writeByte(4));
    }

    @Test
    @DisplayName("String encoding obeys explicit encodings and fallback")
    void stringEncodingObeysExplicitEncodingAndFallback() {
        MessageBuilder builder = new MessageBuilder()
                .writeCode(MessageCode.Peer.INFO_REQUEST)
                .writeString("Ð", CharacterEncoding.getIso88591())
                .writeString("බ", CharacterEncoding.getIso88591())
                .writeString("");
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(builder.build(), MessageCode.Peer.class);

        assertEquals("Ð", reader.readString(CharacterEncoding.getIso88591()));
        assertEquals("බ", reader.readString());
        assertEquals("", reader.readString());
    }

    @Test
    @DisplayName("Reader falls back from malformed UTF-8 to ISO-8859-1")
    void readerFallsBackToIso() {
        byte[] message = new MessageBuilder()
                .writeCode(MessageCode.Peer.INFO_REQUEST)
                .writeInteger(1)
                .writeByte(0xd0)
                .build();
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(message, MessageCode.Peer.class);

        DecodedString decoded = reader.readStringAndEncoding();

        assertEquals("Ð", decoded.value());
        assertSame(CharacterEncoding.getIso88591(), decoded.encoding());
    }

    @Test
    @DisplayName("Compression round trips and enforces state")
    void compressionRoundTripsAndEnforcesState() {
        byte[] message = new MessageBuilder()
                .writeCode(MessageCode.Peer.INFO_REQUEST)
                .writeString("hello")
                .writeInteger(-42)
                .compress()
                .build();
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(message, MessageCode.Peer.class);

        reader.decompress();

        assertEquals("hello", reader.readString());
        assertEquals(-42, reader.readInteger());
        assertThrows(IllegalStateException.class, reader::decompress);
        assertThrows(
                IllegalStateException.class,
                () -> new MessageBuilder()
                        .writeCode(MessageCode.Peer.INFO_REQUEST)
                        .compress());
    }

    @Test
    @DisplayName("Invalid compressed payload is wrapped")
    void invalidCompressedPayloadIsWrapped() {
        byte[] message = new MessageBuilder()
                .writeCode(MessageCode.Peer.INFO_RESPONSE)
                .writeBytes(new byte[] {1, 2, 3})
                .build();
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(message, MessageCode.Peer.class);

        assertThrows(MessageCompressionException.class, reader::decompress);
    }

    @Test
    @DisplayName("Reader tracks position, remaining data, and seeking")
    void readerTracksPositionAndSeeking() {
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(
                new MessageBuilder()
                        .writeCode(MessageCode.Peer.BROWSE_REQUEST)
                        .writeInteger(1)
                        .writeInteger(2)
                        .build(),
                MessageCode.Peer.class);

        assertEquals(8, reader.getLength());
        assertEquals(8, reader.getRemaining());
        assertTrue(reader.hasMoreData());
        assertEquals(1, reader.readInteger());
        assertEquals(4, reader.getPosition());
        reader.seek(8);
        assertFalse(reader.hasMoreData());
        assertEquals(0, reader.getRemaining());
        assertThrows(IllegalArgumentException.class, () -> reader.seek(-1));
        assertThrows(IllegalArgumentException.class, () -> reader.seek(9));
    }

    @Test
    @DisplayName("Reader wraps primitive underflow")
    void readerWrapsPrimitiveUnderflow() {
        byte[] empty =
                new MessageBuilder().writeCode(MessageCode.Peer.BROWSE_REQUEST).build();

        assertThrows(MessageReadException.class, () -> new MessageReader<>(empty, MessageCode.Peer.class).readByte());
        assertThrows(
                MessageReadException.class, () -> new MessageReader<>(empty, MessageCode.Peer.class).readInteger());
        assertThrows(MessageReadException.class, () -> new MessageReader<>(empty, MessageCode.Peer.class).readLong());
        assertThrows(MessageReadException.class, () -> new MessageReader<>(empty, MessageCode.Peer.class).readBytes(1));
    }

    @Test
    @DisplayName("Reader validates construction and string lengths")
    void readerValidatesConstructionAndStringLengths() {
        assertThrows(
                NullPointerException.class, () -> new MessageReader<MessageCode.Peer>(null, MessageCode.Peer.class));
        assertThrows(IllegalArgumentException.class, () -> new MessageReader<>(new byte[7], MessageCode.Peer.class));

        byte[] tooLong = new MessageBuilder()
                .writeCode(MessageCode.Peer.INFO_REQUEST)
                .writeInteger(4)
                .writeBytes(new byte[] {1, 2, 3})
                .build();
        assertThrows(
                MessageReadException.class, () -> new MessageReader<>(tooLong, MessageCode.Peer.class).readString());
    }

    @Test
    @DisplayName("File and directory records round trip")
    void fileAndDirectoryRecordsRoundTrip() {
        File file = new File(
                1,
                "music\\track.mp3",
                123_456,
                "mp3",
                List.of(
                        new FileAttribute(FileAttributeType.BIT_RATE, 320),
                        new FileAttribute(FileAttributeType.LENGTH, 180)));
        Directory directory = new Directory("music", List.of(file));
        byte[] message = new MessageBuilder()
                .writeCode(MessageCode.Peer.BROWSE_RESPONSE)
                .writeDirectory(directory)
                .build();
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(message, MessageCode.Peer.class);

        Directory decoded = reader.readDirectory();

        assertEquals("music", decoded.getName());
        assertEquals(1, decoded.getFileCount());
        File decodedFile = decoded.getFiles().get(0);
        assertEquals(file.getCode(), decodedFile.getCode());
        assertEquals(file.getFilename(), decodedFile.getFilename());
        assertEquals(file.getSize(), decodedFile.getSize());
        assertEquals(file.getExtension(), decodedFile.getExtension());
        assertEquals(2, decodedFile.getAttributeCount());
        assertEquals(320, decodedFile.getBitRate());
        assertEquals(180, decodedFile.getLength());
    }

    @Test
    @DisplayName("File reader repairs sign-extended 32-bit sizes")
    void fileReaderRepairsSignExtendedSize() {
        long signExtended = (long) (int) 3_000_000_000L;
        byte[] message = new MessageBuilder()
                .writeCode(MessageCode.Peer.SEARCH_RESPONSE)
                .writeByte(1)
                .writeString("large.bin")
                .writeLong(signExtended)
                .writeString("bin")
                .writeInteger(0)
                .build();
        File file = new MessageReader<>(message, MessageCode.Peer.class).readFile();

        assertEquals(3_000_000_000L, file.getSize());
    }

    /**
     * Peers run clients newer than this one, and the C# source reads their
     * attribute types tolerantly. Throwing instead meant one nonstandard
     * attribute discarded the whole search or browse response it arrived in.
     */
    @Test
    @DisplayName("File reader skips an unknown attribute type and keeps the file")
    void fileReaderSkipsUnknownAttributeTypes() {
        byte[] message = new MessageBuilder()
                .writeCode(MessageCode.Peer.SEARCH_RESPONSE)
                .writeByte(1)
                .writeString("song.mp3")
                .writeLong(1_000L)
                .writeString("mp3")
                .writeInteger(3)
                .writeInteger(0) // BIT_RATE
                .writeInteger(320)
                .writeInteger(6) // an attribute type this client does not know
                .writeInteger(999)
                .writeInteger(1) // LENGTH — after the unknown one, so framing is proven intact
                .writeInteger(180)
                .build();

        File file = new MessageReader<>(message, MessageCode.Peer.class).readFile();

        assertEquals("song.mp3", file.getFilename());
        assertEquals(2, file.getAttributeCount());
        assertEquals(320, file.getBitRate());
        assertEquals(180, file.getLength());
    }

    @Test
    @DisplayName("String null and unknown codes preserve failure behavior")
    void stringNullAndUnknownCodesFail() {
        assertThrows(NullPointerException.class, () -> new MessageBuilder().writeString(null));

        byte[] unknown = {
            4, 0, 0, 0,
            2, 0, 0, 0
        };
        assertThrows(
                IllegalArgumentException.class, () -> new MessageReader<>(unknown, MessageCode.Peer.class).readCode());
    }

    @Test
    @DisplayName("Payload contents are independent snapshots")
    void payloadIsSnapshot() {
        byte[] content = "abc".getBytes(StandardCharsets.US_ASCII);
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(
                new MessageBuilder()
                        .writeCode(MessageCode.Peer.INFO_REQUEST)
                        .writeBytes(content)
                        .build(),
                MessageCode.Peer.class);
        byte[] first = reader.getPayload();
        first[0] = 0;

        assertArrayEquals(content, reader.getPayload());
    }
}
