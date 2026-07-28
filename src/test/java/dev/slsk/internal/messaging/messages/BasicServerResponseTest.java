// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BasicServerResponseTest {
    @Test
    @DisplayName("Integer response parses the payload without code validation")
    void integerResponseParsesPayload() {
        byte[] message = new MessageBuilder()
                .writeCode(MessageCode.Server.GET_PEER_ADDRESS)
                .writeInteger(0x12345678)
                .build();

        assertEquals(0x12345678, IntegerResponse.fromByteArray(message, MessageCode.Server.class));
    }

    @Test
    @DisplayName("Integer response preserves missing-data failure")
    void integerResponseRejectsMissingData() {
        byte[] message = new MessageBuilder()
                .writeCode(MessageCode.Server.GET_PEER_ADDRESS)
                .build();

        assertThrows(
                MessageReadException.class, () -> IntegerResponse.fromByteArray(message, MessageCode.Server.class));
    }

    @Test
    @DisplayName("String response parses the payload without code validation")
    void stringResponseParsesPayload() {
        byte[] message = new MessageBuilder()
                .writeCode(MessageCode.Server.GET_PEER_ADDRESS)
                .writeString("pässword")
                .build();

        assertEquals("pässword", StringResponse.fromByteArray(message, MessageCode.Server.class));
    }

    @Test
    @DisplayName("String response preserves missing-data failure")
    void stringResponseRejectsMissingData() {
        byte[] message = new MessageBuilder()
                .writeCode(MessageCode.Server.GET_PEER_ADDRESS)
                .build();

        assertThrows(MessageReadException.class, () -> StringResponse.fromByteArray(message, MessageCode.Server.class));
    }

    @Test
    @DisplayName("Server ping has the exact empty frame and round trips")
    void serverPingRoundTrips() {
        byte[] bytes = new ServerPing().toByteArray();

        assertArrayEquals(new byte[] {4, 0, 0, 0, 32, 0, 0, 0}, bytes);
        assertInstanceOf(ServerPing.class, ServerPing.fromByteArray(bytes));
    }

    @Test
    @DisplayName("Server ping rejects a mismatched code")
    void serverPingRejectsCodeMismatch() {
        byte[] message = new MessageBuilder()
                .writeCode(MessageCode.Server.BRANCH_LEVEL)
                .writeInteger(1)
                .build();

        MessageException exception = assertThrows(MessageException.class, () -> ServerPing.fromByteArray(message));
        assertEquals(
                "Message Code mismatch creating ServerPing " + "(expected: 32, received: 126)", exception.getMessage());
    }

    @Test
    @DisplayName("New password retains data and round trips exactly")
    void newPasswordRoundTrips() {
        NewPassword message = new NewPassword("sëcret");

        assertEquals("sëcret", message.getPassword());
        assertEquals("sëcret", NewPassword.fromByteArray(message.toByteArray()).getPassword());
        assertArrayEquals(
                new MessageBuilder()
                        .writeCode(MessageCode.Server.NEW_PASSWORD)
                        .writeString("sëcret")
                        .build(),
                message.toByteArray());
    }

    @Test
    @DisplayName("New password rejects a cross-family code")
    void newPasswordRejectsCodeMismatch() {
        byte[] message =
                new MessageBuilder().writeCode(MessageCode.Peer.BROWSE_REQUEST).build();

        assertThrows(MessageException.class, () -> NewPassword.fromByteArray(message));
    }

    @Test
    @DisplayName("New password preserves missing-data failure")
    void newPasswordRejectsMissingData() {
        byte[] message =
                new MessageBuilder().writeCode(MessageCode.Server.NEW_PASSWORD).build();

        assertThrows(MessageReadException.class, () -> NewPassword.fromByteArray(message));
    }
}
