// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.user.UserInfoMessage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class UserInfoResponseFactoryTest {
    @Test
    @DisplayName("UserInfoMessage with a picture preserves its exact wire format")
    void responseWithPicturePreservesWireFormat() {
        byte[] picture = {1, 2, (byte) 0xff};
        UserInfoMessage outgoing = new UserInfoMessage("d", 2, 3, true, picture);
        byte[] bytes = outgoing.toByteArray();

        assertArrayEquals(
                new byte[] {
                    26,
                    0,
                    0,
                    0,
                    16,
                    0,
                    0,
                    0,
                    1,
                    0,
                    0,
                    0,
                    'd',
                    1,
                    3,
                    0,
                    0,
                    0,
                    1,
                    2,
                    (byte) 0xff,
                    2,
                    0,
                    0,
                    0,
                    3,
                    0,
                    0,
                    0,
                    1
                },
                bytes);

        UserInfoMessage parsed = UserInfoResponseFactory.fromByteArray(bytes);
        assertEquals("d", parsed.description());
        assertTrue(parsed.hasPicture());
        assertArrayEquals(picture, parsed.picture());
        assertEquals(2, parsed.uploadSlots());
        assertEquals(3, parsed.queueLength());
        assertTrue(parsed.freeUploadSlot());
        assertArrayEquals(picture, outgoing.picture());
    }

    @Test
    @DisplayName("UserInfoMessage without a picture preserves its exact wire format")
    void responseWithoutPicturePreservesWireFormat() {
        UserInfoMessage outgoing = new UserInfoMessage("d", -2, -3, false);
        byte[] bytes = outgoing.toByteArray();

        assertArrayEquals(
                new byte[] {
                    19,
                    0,
                    0,
                    0,
                    16,
                    0,
                    0,
                    0,
                    1,
                    0,
                    0,
                    0,
                    'd',
                    0,
                    (byte) 0xfe,
                    (byte) 0xff,
                    (byte) 0xff,
                    (byte) 0xff,
                    (byte) 0xfd,
                    (byte) 0xff,
                    (byte) 0xff,
                    (byte) 0xff,
                    0
                },
                bytes);

        UserInfoMessage parsed = UserInfoResponseFactory.fromByteArray(bytes);
        assertEquals("d", parsed.description());
        assertFalse(parsed.hasPicture());
        assertNull(parsed.picture());
        assertEquals(-2, parsed.uploadSlots());
        assertEquals(-3, parsed.queueLength());
        assertFalse(parsed.freeUploadSlot());
    }

    @Test
    @DisplayName("Parser treats any positive flag byte as true")
    void parserTreatsPositiveFlagBytesAsTrue() {
        byte[] bytes = new MessageBuilder()
                .writeCode(MessageCode.Peer.INFO_RESPONSE)
                .writeString("d")
                .writeByte(2)
                .writeInteger(0)
                .writeInteger(1)
                .writeInteger(2)
                .writeByte(255)
                .build();

        UserInfoMessage parsed = UserInfoResponseFactory.fromByteArray(bytes);

        assertTrue(parsed.hasPicture());
        assertEquals(0, parsed.picture().length);
        assertTrue(parsed.freeUploadSlot());
    }

    @Test
    @DisplayName("Parser rejects code mismatch and missing data")
    void parserRejectsInvalidData() {
        assertThrows(
                MessageException.class,
                () -> UserInfoResponseFactory.fromByteArray(new BrowseRequestMessage().toByteArray()));
        byte[] missing = new MessageBuilder()
                .writeCode(MessageCode.Peer.INFO_RESPONSE)
                .writeString("d")
                .build();
        assertThrows(MessageReadException.class, () -> UserInfoResponseFactory.fromByteArray(missing));
    }

    @Test
    @DisplayName("Factory rejects null response serialization")
    void factoryRejectsNullSerialization() {
        assertThrows(NullPointerException.class, () -> UserInfoResponseFactory.toByteArray(null));
    }
}
