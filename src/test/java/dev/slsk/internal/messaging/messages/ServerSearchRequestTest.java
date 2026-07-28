// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ServerSearchRequestTest {
    @Test
    @DisplayName("Constructor retains server-routed search data")
    void constructorRetainsData() {
        ServerSearchRequest request = new ServerSearchRequest("alice", -42, "música");

        assertEquals("alice", request.getUsername());
        assertEquals(-42, request.getToken());
        assertEquals("música", request.getQuery());
    }

    @Test
    @DisplayName("Parser reads username, token, and query in order")
    void parserReadsFields() {
        ServerSearchRequest request = ServerSearchRequest.fromByteArray(new MessageBuilder()
                .writeCode(MessageCode.Server.FILE_SEARCH)
                .writeString("alice")
                .writeInteger(-42)
                .writeString("música")
                .build());

        assertEquals("alice", request.getUsername());
        assertEquals(-42, request.getToken());
        assertEquals("música", request.getQuery());
    }

    @Test
    @DisplayName("Parser rejects mismatches and missing data")
    void parserRejectsInvalidFrames() {
        assertThrows(
                MessageException.class,
                () -> ServerSearchRequest.fromByteArray(new MessageBuilder()
                        .writeCode(MessageCode.Peer.BROWSE_REQUEST)
                        .build()));
        assertThrows(
                MessageReadException.class,
                () -> ServerSearchRequest.fromByteArray(new MessageBuilder()
                        .writeCode(MessageCode.Server.FILE_SEARCH)
                        .build()));
    }
}
