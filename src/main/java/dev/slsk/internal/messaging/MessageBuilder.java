// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging;

import dev.slsk.exceptions.MessageCompressionException;
import dev.slsk.internal.CharacterEncoding;
import dev.slsk.internal.share.Directory;
import dev.slsk.internal.share.File;
import dev.slsk.internal.share.FileAttribute;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.zip.DeflaterOutputStream;

/**
 * Builds a framed Soulseek protocol message.
 */
public final class MessageBuilder {
    private byte[] codeBytes = new byte[0];
    private boolean compressed;
    private ByteArrayOutputStream payload = new ByteArrayOutputStream();

    /**
     * Builds the message with its little-endian length prefix.
     *
     * @return the framed message
     */
    public byte[] build() {
        if (codeBytes.length == 0) {
            throw new IllegalStateException("Unable to build the message without a message code");
        }

        byte[] payloadBytes = payload.toByteArray();
        ByteBuffer result =
                ByteBuffer.allocate(4 + codeBytes.length + payloadBytes.length).order(ByteOrder.LITTLE_ENDIAN);
        result.putInt(codeBytes.length + payloadBytes.length);
        result.put(codeBytes);
        result.put(payloadBytes);
        return result.array();
    }

    /**
     * Compresses the payload using zlib framing.
     *
     * @return this builder
     */
    public MessageBuilder compress() {
        byte[] input = payload.toByteArray();
        if (input.length == 0) {
            throw new IllegalStateException("Unable to compress an empty message");
        }
        if (compressed) {
            throw new IllegalStateException("The message has already been compressed");
        }

        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            try (DeflaterOutputStream deflater = new DeflaterOutputStream(output)) {
                deflater.write(input);
                deflater.finish();
            }
            payload = output;
            compressed = true;
            return this;
        } catch (IOException exception) {
            throw new MessageCompressionException("Failed to compress the message payload", exception);
        }
    }

    /**
     * Writes one byte.
     *
     * @param value the low eight bits to write
     * @return this builder
     */
    public MessageBuilder writeByte(int value) {
        return writeBytes(new byte[] {(byte) value});
    }

    /**
     * Appends bytes to the payload.
     *
     * @param bytes the bytes
     * @return this builder
     */
    public MessageBuilder writeBytes(byte[] bytes) {
        if (compressed) {
            throw new IllegalStateException("Unable to write data after message compression");
        }
        Objects.requireNonNull(bytes, "bytes");
        payload.writeBytes(bytes);
        return this;
    }

    /**
     * Sets or replaces the message code.
     *
     * @param code the message code
     * @return this builder
     */
    public MessageBuilder writeCode(ProtocolCode code) {
        Objects.requireNonNull(code, "code");
        if (code.getByteLength() == 1) {
            codeBytes = new byte[] {(byte) code.getValue()};
        } else if (code.getByteLength() == 4) {
            codeBytes = littleEndianInteger(code.getValue());
        } else {
            throw new IllegalArgumentException("Unsupported message-code width: " + code.getByteLength());
        }
        return this;
    }

    /**
     * Writes a 32-bit little-endian integer.
     */
    public MessageBuilder writeInteger(int value) {
        return writeBytes(littleEndianInteger(value));
    }

    /**
     * Writes a 64-bit little-endian integer.
     */
    public MessageBuilder writeLong(long value) {
        return writeBytes(ByteBuffer.allocate(Long.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(value)
                .array());
    }

    /**
     * Writes a length-prefixed UTF-8 string.
     */
    public MessageBuilder writeString(String value) {
        return writeString(value, null);
    }

    /**
     * Writes a length-prefixed string.
     *
     * <p>The requested encoding is strict. Encoding failure falls up to
     * replacement-tolerant UTF-8, matching the source.
     */
    public MessageBuilder writeString(String value, CharacterEncoding encoding) {
        CharacterEncoding requested = encoding == null ? CharacterEncoding.getUtf8() : encoding;
        byte[] bytes;

        try {
            bytes = strictEncode(value, requested);
        } catch (RuntimeException | CharacterCodingException exception) {
            bytes = replacementUtf8(value);
        }

        return writeInteger(bytes.length).writeBytes(bytes);
    }

    /**
     * Writes a file record.
     */
    public MessageBuilder writeFile(File file) {
        Objects.requireNonNull(file, "file");
        writeByte(file.getCode())
                .writeString(file.getFilename())
                .writeLong(file.getSize())
                .writeString(file.getExtension())
                .writeInteger(file.getAttributeCount());

        for (FileAttribute attribute : file.getAttributes()) {
            writeInteger(attribute.getType().getValue()).writeInteger(attribute.getValue());
        }
        return this;
    }

    /**
     * Writes a directory record.
     */
    public MessageBuilder writeDirectory(Directory directory) {
        Objects.requireNonNull(directory, "directory");
        writeString(directory.getName()).writeInteger(directory.getFileCount());
        for (File file : directory.getFiles()) {
            writeFile(file);
        }
        return this;
    }

    private static byte[] strictEncode(String value, CharacterEncoding encoding) throws CharacterCodingException {
        ByteBuffer buffer = Charset.forName(encoding.toString())
                .newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .encode(CharBuffer.wrap(value));
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private static byte[] replacementUtf8(String value) {
        try {
            ByteBuffer buffer = StandardCharsets.UTF_8
                    .newEncoder()
                    .onMalformedInput(CodingErrorAction.REPLACE)
                    .onUnmappableCharacter(CodingErrorAction.REPLACE)
                    .replaceWith(new byte[] {(byte) 0xef, (byte) 0xbf, (byte) 0xbd})
                    .encode(CharBuffer.wrap(value));
            byte[] result = new byte[buffer.remaining()];
            buffer.get(result);
            return result;
        } catch (CharacterCodingException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static byte[] littleEndianInteger(int value) {
        return ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(value)
                .array();
    }
}
