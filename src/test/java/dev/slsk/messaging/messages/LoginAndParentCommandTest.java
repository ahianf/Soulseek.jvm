// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;
import java.net.InetAddress;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class LoginAndParentCommandTest {
    @Test
    @DisplayName("LoginRequest preserves credentials versions and MD5 hash")
    void loginRequestPreservesData() {
        LoginRequest request = new LoginRequest(9999, "alice", "secret");

        assertEquals("alice", request.getUsername());
        assertEquals("secret", request.getPassword());
        assertEquals(170, request.getVersion());
        assertEquals(9999, request.getMinorVersion());
        assertEquals("c4e31313222cf05fcdd1fc068af5570e", request.getHash());

        MessageReader<MessageCode.Server> reader = new MessageReader<>(request.toByteArray(), MessageCode.Server.class);
        assertEquals(MessageCode.Server.LOGIN, reader.readCode());
        assertEquals("alice", reader.readString());
        assertEquals("secret", reader.readString());
        assertEquals(170, reader.readInteger());
        assertEquals(request.getHash(), reader.readString());
        assertEquals(9999, reader.readInteger());
        assertEquals(0, reader.getRemaining());
    }

    @Test
    @DisplayName("LoginRequest preserves null interpolation in its hash")
    void loginRequestPreservesNullInterpolation() {
        LoginRequest request = new LoginRequest(0, null, null);

        assertNull(request.getUsername());
        assertNull(request.getPassword());
        assertEquals("d41d8cd98f00b204e9800998ecf8427e", request.getHash());
        assertThrows(NullPointerException.class, request::toByteArray);
    }

    @Test
    @DisplayName("ParentsIP reverses IPv4 bytes without mutating address")
    void parentsIpPreservesIpv4WireFormat() throws Exception {
        InetAddress address = InetAddress.getByAddress(new byte[] {1, 2, 3, 4});
        ParentsIPCommand command = new ParentsIPCommand(address);

        assertSame(address, command.getIpAddress());
        assertArrayEquals(new byte[] {1, 2, 3, 4}, address.getAddress());
        assertArrayEquals(
                new byte[] {
                    8, 0, 0, 0,
                    73, 0, 0, 0,
                    4, 3, 2, 1
                },
                command.toByteArray());
    }

    @Test
    @DisplayName("ParentsIP without an address sends an empty payload")
    void parentsIpPreservesNullDefault() {
        ParentsIPCommand command = new ParentsIPCommand();

        assertNull(command.getIpAddress());
        assertArrayEquals(
                new byte[] {
                    4, 0, 0, 0,
                    73, 0, 0, 0
                },
                command.toByteArray());
    }

    @Test
    @DisplayName("ParentsIP reverses the complete source address byte array")
    void parentsIpPreservesIpv6SourceBehavior() throws Exception {
        byte[] source = {
            0, 1, 2, 3, 4, 5, 6, 7,
            8, 9, 10, 11, 12, 13, 14, 15
        };
        ParentsIPCommand command = new ParentsIPCommand(InetAddress.getByAddress(source));
        MessageReader<MessageCode.Server> reader = new MessageReader<>(command.toByteArray(), MessageCode.Server.class);

        assertEquals(MessageCode.Server.PARENTS_IP, reader.readCode());
        assertArrayEquals(
                new byte[] {
                    15, 14, 13, 12, 11, 10, 9, 8,
                    7, 6, 5, 4, 3, 2, 1, 0
                },
                reader.readBytes(16));
    }
}
