// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.messaging.MessageCode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class InitializationMessageTest {
    @Test
    @DisplayName("PeerInit retains constructor data")
    void peerInitRetainsConstructorData() {
        PeerInit message = new PeerInit("alice", "P", -123456789);

        assertEquals("alice", message.getUsername());
        assertEquals("P", message.getConnectionType());
        assertEquals(-123456789, message.getToken());
    }

    @Test
    @DisplayName("PeerInit serializes to the source wire format")
    void peerInitSerializesToWireFormat() {
        byte[] expected = {19, 0, 0, 0, 1, 5, 0, 0, 0, 'a', 'l', 'i', 'c', 'e', 1, 0, 0, 0, 'P', 0x78, 0x56, 0x34, 0x12
        };

        assertArrayEquals(expected, new PeerInit("alice", "P", 0x12345678).toByteArray());
    }

    @Test
    @DisplayName("PeerInit parses the source wire format")
    void peerInitParsesWireFormat() {
        PeerInit message = PeerInit.tryFromByteArray(peerInitBytes("bob", "F", Integer.MIN_VALUE))
                .orElseThrow();

        assertEquals("bob", message.getUsername());
        assertEquals("F", message.getConnectionType());
        assertEquals(Integer.MIN_VALUE, message.getToken());
    }

    @Test
    @DisplayName("PeerInit parse returns empty for mismatch or malformed data")
    void peerInitParseReturnsEmptyForFailure() {
        assertFalse(PeerInit.tryFromByteArray(pierceFirewallBytes(42)).isPresent());
        byte[] truncated = peerInitBytes("bob", "F", 42);
        assertFalse(PeerInit.tryFromByteArray(java.util.Arrays.copyOf(truncated, truncated.length - 1))
                .isPresent());
        assertFalse(PeerInit.tryFromByteArray(null).isPresent());
    }

    @Test
    @DisplayName("PierceFirewall retains and serializes its token")
    void pierceFirewallRetainsAndSerializesToken() {
        PierceFirewall message = new PierceFirewall(0x12345678);

        assertEquals(0x12345678, message.getToken());
        assertArrayEquals(new byte[] {5, 0, 0, 0, 0, 0x78, 0x56, 0x34, 0x12}, message.toByteArray());
    }

    @Test
    @DisplayName("PierceFirewall parses the source wire format")
    void pierceFirewallParsesWireFormat() {
        PierceFirewall message =
                PierceFirewall.tryFromByteArray(pierceFirewallBytes(-17)).orElseThrow();

        assertEquals(-17, message.getToken());
    }

    @Test
    @DisplayName("PierceFirewall parse returns empty for mismatch or malformed data")
    void pierceFirewallParseReturnsEmptyForFailure() {
        assertFalse(
                PierceFirewall.tryFromByteArray(peerInitBytes("bob", "P", 42)).isPresent());
        assertFalse(PierceFirewall.tryFromByteArray(new byte[] {1, 0, 0, 0, 0}).isPresent());
        assertFalse(PierceFirewall.tryFromByteArray(null).isPresent());
    }

    @Test
    @DisplayName("Initialization messages implement the source marker")
    void initializationMessagesImplementMarker() {
        assertTrue(new PeerInit("u", "P", 1) instanceof InitializationMessage);
        assertTrue(new PierceFirewall(1) instanceof InitializationMessage);
    }

    private static byte[] peerInitBytes(String username, String connectionType, int token) {
        byte[] usernameBytes = username.getBytes(StandardCharsets.UTF_8);
        byte[] connectionBytes = connectionType.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(4 + 1 + 4 + usernameBytes.length + 4 + connectionBytes.length + 4)
                .order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(buffer.capacity() - 4);
        buffer.put((byte) MessageCode.Initialization.PEER_INIT.getValue());
        buffer.putInt(usernameBytes.length);
        buffer.put(usernameBytes);
        buffer.putInt(connectionBytes.length);
        buffer.put(connectionBytes);
        buffer.putInt(token);
        return buffer.array();
    }

    private static byte[] pierceFirewallBytes(int token) {
        return ByteBuffer.allocate(9)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(5)
                .put((byte) MessageCode.Initialization.PIERCE_FIREWALL.getValue())
                .putInt(token)
                .array();
    }
}
