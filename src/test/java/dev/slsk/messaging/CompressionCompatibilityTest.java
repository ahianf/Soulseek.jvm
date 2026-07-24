// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.Test;

class CompressionCompatibilityTest {
    private static final int TOKEN = 0x89abcdef;
    private static final long SIZE = 0x0123456789abcdefL;
    private static final String TEXT = "Soulseek.jvm C# ↔ Java zlib ✓";

    @Test
    void csharpCompressedPayloadDecodesInJava() throws IOException {
        verify(fixture("csharp-compressed-message.base64"));
    }

    @Test
    void javaCompressedPayloadIsStableAndDecodable() throws IOException {
        byte[] expected = fixture("java-compressed-message.base64");
        byte[] actual = buildJavaMessage();

        assertArrayEquals(expected, actual);
        verify(actual);
    }

    private static byte[] buildJavaMessage() {
        return new MessageBuilder()
                .writeCode(MessageCode.Peer.FOLDER_CONTENTS_RESPONSE)
                .writeInteger(TOKEN)
                .writeLong(SIZE)
                .writeString(TEXT)
                .writeBytes(tail())
                .compress()
                .build();
    }

    private static void verify(byte[] message) {
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(message, MessageCode.Peer.class);
        assertEquals(MessageCode.Peer.FOLDER_CONTENTS_RESPONSE, reader.readCode());

        reader.decompress();
        assertEquals(TOKEN, reader.readInteger());
        assertEquals(SIZE, reader.readLong());
        assertEquals(TEXT, reader.readString());
        assertArrayEquals(tail(), reader.readBytes(256));
        assertFalse(reader.hasMoreData());
    }

    private static byte[] fixture(String name) throws IOException {
        try (InputStream stream = CompressionCompatibilityTest.class.getResourceAsStream("/compatibility/" + name)) {
            assertNotNull(stream, name);
            String base64 = new String(stream.readAllBytes(), StandardCharsets.US_ASCII).trim();
            return Base64.getDecoder().decode(base64);
        }
    }

    private static byte[] tail() {
        byte[] result = new byte[256];
        for (int index = 0; index < result.length; index++) {
            result[index] = (byte) index;
        }
        return result;
    }
}
