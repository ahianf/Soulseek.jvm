// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging;

import dev.slsk.CharacterEncoding;
import dev.slsk.Directory;
import dev.slsk.File;
import dev.slsk.FileAttribute;
import dev.slsk.FileAttributeType;
import dev.slsk.exceptions.MessageCompressionException;
import dev.slsk.exceptions.MessageReadException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.zip.InflaterInputStream;

/**
 * Reads values from a framed Soulseek protocol message.
 *
 * @param <T> the message-code enum
 */
public final class MessageReader<T extends Enum<T> & ProtocolCode> {
    private final int codeLength;
    private final Class<T> codeType;
    private final byte[] message;
    private boolean decompressed;
    private byte[] payload;
    private int position;

    /**
     * Creates a message reader.
     *
     * @param bytes the framed message
     * @param codeType the message-code enum type
     */
    public MessageReader(byte[] bytes, Class<T> codeType) {
        Objects.requireNonNull(bytes, "bytes");
        this.codeType = Objects.requireNonNull(codeType, "codeType");
        T[] codes = codeType.getEnumConstants();
        if (codes == null || codes.length == 0) {
            throw new IllegalArgumentException("codeType must be a non-empty protocol-code enum");
        }
        codeLength = codes[0].getByteLength();
        if (bytes.length < 4 + codeLength) {
            throw new IllegalArgumentException(
                    "Message length is less than the minimum of " + (4 + codeLength) + " bytes");
        }

        message = bytes;
        payload = Arrays.copyOfRange(bytes, 4 + codeLength, bytes.length);
    }

    /**
     * Returns whether unread payload data remains.
     */
    public boolean isHasMoreData() {
        return position < payload.length;
    }

    /**
     * Returns the payload length.
     */
    public int getLength() {
        return payload.length;
    }

    /**
     * Returns a copy of the payload.
     */
    public byte[] getPayload() {
        return payload.clone();
    }

    /**
     * Returns the current payload position.
     */
    public int getPosition() {
        return position;
    }

    /**
     * Returns the unread payload length.
     */
    public int getRemaining() {
        return payload.length - position;
    }

    /**
     * Decompresses the zlib-framed payload.
     */
    public MessageReader<T> decompress() {
        if (payload.length == 0) {
            throw new IllegalStateException("Unable to decompress an empty message");
        }
        if (decompressed) {
            throw new IllegalStateException("The message has already been decompressed");
        }

        try (InflaterInputStream inflater = new InflaterInputStream(new ByteArrayInputStream(payload));
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            inflater.transferTo(output);
            payload = output.toByteArray();
            decompressed = true;
            return this;
        } catch (IOException exception) {
            throw new MessageCompressionException("Failed to decompress the message payload", exception);
        }
    }

    /**
     * Reads an unsigned byte.
     */
    public int readByte() {
        try {
            int result = Byte.toUnsignedInt(payload[position]);
            position++;
            return result;
        } catch (RuntimeException exception) {
            throw new MessageReadException("Failed to read a byte from position " + position, exception);
        }
    }

    /**
     * Reads a byte sequence.
     */
    public byte[] readBytes(int count) {
        if (count < 0) {
            throw new IllegalArgumentException("count must not be negative");
        }
        if (count > payload.length - position) {
            throw new MessageReadException("Requested bytes extend beyond the message payload");
        }
        byte[] result = Arrays.copyOfRange(payload, position, position + count);
        position += count;
        return result;
    }

    /**
     * Reads the message code without advancing the payload position.
     */
    public T readCode() {
        int value = codeLength == 1
                ? Byte.toUnsignedInt(message[4])
                : ByteBuffer.wrap(message, 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        for (T code : codeType.getEnumConstants()) {
            if (code.getValue() == value) {
                return code;
            }
        }
        throw new IllegalArgumentException("Unknown " + codeType.getSimpleName() + " code: " + value);
    }

    /**
     * Reads a 32-bit little-endian integer.
     */
    public int readInteger() {
        try {
            int result = ByteBuffer.wrap(payload, position, Integer.BYTES)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .getInt();
            position += Integer.BYTES;
            return result;
        } catch (RuntimeException exception) {
            throw new MessageReadException("Failed to read an integer from position " + position, exception);
        }
    }

    /**
     * Reads a 64-bit little-endian integer.
     */
    public long readLong() {
        try {
            long result = ByteBuffer.wrap(payload, position, Long.BYTES)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .getLong();
            position += Long.BYTES;
            return result;
        } catch (RuntimeException exception) {
            throw new MessageReadException("Failed to read a long integer from position " + position, exception);
        }
    }

    /**
     * Reads a length-prefixed string using UTF-8 with ISO fallback.
     */
    public String readString() {
        return readString(null);
    }

    /**
     * Reads a length-prefixed string.
     */
    public String readString(CharacterEncoding encoding) {
        return readStringAndEncoding(encoding).value();
    }

    /**
     * Reads a string and returns the encoding that succeeded.
     */
    public DecodedString readStringAndEncoding() {
        return readStringAndEncoding(null);
    }

    /**
     * Reads a string and returns the encoding that succeeded.
     */
    public DecodedString readStringAndEncoding(CharacterEncoding encoding) {
        int length = readInteger();
        if (length < 0) {
            throw new IllegalArgumentException("Specified string length must not be negative");
        }
        if (length > payload.length - position) {
            throw new MessageReadException("Specified string length extends beyond the message payload");
        }

        CharacterEncoding selected = encoding == null ? CharacterEncoding.getUtf8() : encoding;
        byte[] bytes = Arrays.copyOfRange(payload, position, position + length);
        String value;

        try {
            value = strictDecode(bytes, selected);
        } catch (RuntimeException | CharacterCodingException exception) {
            selected = CharacterEncoding.getIso88591();
            value = Charset.forName(selected.toString())
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        }

        position += length;
        return new DecodedString(value, selected);
    }

    /**
     * Moves the payload position.
     */
    public void seek(int newPosition) {
        if (newPosition < 0) {
            throw new IllegalArgumentException("Cannot seek to a negative position");
        }
        if (newPosition > payload.length) {
            throw new IllegalArgumentException("Seek would extend beyond the message payload");
        }
        position = newPosition;
    }

    /**
     * Reads a file record.
     */
    public File readFile() {
        int code = readByte();
        String filename = readString();
        long size = readLong();
        String extension = readString();

        if (size < 0 && (size >>> 32) == 0xffff_ffffL) {
            size = Integer.toUnsignedLong((int) size);
        }

        int attributeCount = readInteger();
        List<FileAttribute> attributes = new ArrayList<>();
        for (int index = 0; index < attributeCount; index++) {
            attributes.add(new FileAttribute(FileAttributeType.fromValue(readInteger()), readInteger()));
        }
        return new File(code, filename, size, extension, attributes);
    }

    /**
     * Reads a fixed number of file records.
     */
    public List<File> readFiles(int count) {
        List<File> files = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            files.add(readFile());
        }
        return List.copyOf(files);
    }

    /**
     * Reads a directory record.
     */
    public Directory readDirectory() {
        String name = readString();
        int fileCount = readInteger();
        return new Directory(name, readFiles(fileCount));
    }

    private static String strictDecode(byte[] bytes, CharacterEncoding encoding) throws CharacterCodingException {
        return Charset.forName(encoding.toString())
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString();
    }
}
