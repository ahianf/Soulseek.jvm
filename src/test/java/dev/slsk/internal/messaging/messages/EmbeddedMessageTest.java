// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EmbeddedMessageTest {
    @Test
    @DisplayName("Constructor retains source array identity")
    void constructorRetainsData() {
        byte[] bytes = {1, 2, 3};
        EmbeddedMessage message = new EmbeddedMessage(MessageCode.Distributed.SEARCH_REQUEST, bytes);

        assertEquals(MessageCode.Distributed.SEARCH_REQUEST, message.getDistributedCode());
        assertSame(bytes, message.getDistributedMessage());
        assertThrows(NullPointerException.class, () -> new EmbeddedMessage(null, bytes));
    }

    @Test
    @DisplayName("Parser reconstructs the distributed frame exactly")
    void parserReconstructsDistributedFrame() {
        byte[] serverMessage = new MessageBuilder()
                .writeCode(MessageCode.Server.EMBEDDED_MESSAGE)
                .writeByte(MessageCode.Distributed.SEARCH_REQUEST.getValue())
                .writeBytes(new byte[] {1, 2, 3})
                .build();

        assertArrayEquals(
                new byte[] {
                    8, 0, 0, 0,
                    93, 0, 0, 0,
                    3, 1, 2, 3
                },
                serverMessage);

        EmbeddedMessage parsed = EmbeddedMessage.fromByteArray(serverMessage);
        assertEquals(MessageCode.Distributed.SEARCH_REQUEST, parsed.getDistributedCode());
        assertArrayEquals(
                new byte[] {
                    4, 0, 0, 0,
                    3, 1, 2, 3
                },
                parsed.getDistributedMessage());
    }

    @Test
    @DisplayName("Parser rejects a mismatched server message code")
    void parserRejectsCodeMismatch() {
        MessageException exception = assertThrows(
                MessageException.class, () -> EmbeddedMessage.fromByteArray(new BrowseRequestMessage().toByteArray()));

        assertEquals(
                "Message Code mismatch creating EmbeddedMessage " + "(expected: 93, received: 4)",
                exception.getMessage());
    }

    @Test
    @DisplayName("Parser preserves missing distributed code failure")
    void parserPreservesMissingCodeFailure() {
        byte[] missing = new MessageBuilder()
                .writeCode(MessageCode.Server.EMBEDDED_MESSAGE)
                .build();

        assertThrows(MessageReadException.class, () -> EmbeddedMessage.fromByteArray(missing));
    }
}
