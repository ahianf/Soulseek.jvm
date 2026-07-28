// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DistributedMessageTest {
    @Test
    @DisplayName("DistributedBranchLevel preserves data and wire format")
    void branchLevelPreservesDataAndWireFormat() {
        DistributedBranchLevel outgoing = new DistributedBranchLevel(0x12345678);
        byte[] bytes = outgoing.toByteArray();

        assertEquals(0x12345678, outgoing.getLevel());
        assertArrayEquals(new byte[] {5, 0, 0, 0, 4, 0x78, 0x56, 0x34, 0x12}, bytes);
        assertEquals(0x12345678, DistributedBranchLevel.fromByteArray(bytes).getLevel());
    }

    @Test
    @DisplayName("DistributedBranchRoot preserves data and wire format")
    void branchRootPreservesDataAndWireFormat() {
        DistributedBranchRoot outgoing = new DistributedBranchRoot("alice");
        byte[] bytes = outgoing.toByteArray();

        assertEquals("alice", outgoing.getUsername());
        assertArrayEquals(new byte[] {10, 0, 0, 0, 5, 5, 0, 0, 0, 'a', 'l', 'i', 'c', 'e'}, bytes);
        assertEquals("alice", DistributedBranchRoot.fromByteArray(bytes).getUsername());
    }

    @Test
    @DisplayName("DistributedChildDepth preserves data and wire format")
    void childDepthPreservesDataAndWireFormat() {
        DistributedChildDepth outgoing = new DistributedChildDepth(Integer.MIN_VALUE);
        byte[] bytes = outgoing.toByteArray();

        assertEquals(Integer.MIN_VALUE, outgoing.getDepth());
        assertArrayEquals(new byte[] {5, 0, 0, 0, 7, 0, 0, 0, (byte) 0x80}, bytes);
        assertEquals(
                Integer.MIN_VALUE, DistributedChildDepth.fromByteArray(bytes).getDepth());
    }

    @Test
    @DisplayName("DistributedPingRequest preserves its empty wire format")
    void pingRequestPreservesWireFormat() {
        DistributedPingRequest outgoing = new DistributedPingRequest();
        byte[] bytes = outgoing.toByteArray();

        assertArrayEquals(new byte[] {1, 0, 0, 0, 0}, bytes);
        assertInstanceOf(DistributedPingRequest.class, DistributedPingRequest.fromByteArray(bytes));
    }

    @Test
    @DisplayName("DistributedPingResponse preserves data and wire format")
    void pingResponsePreservesDataAndWireFormat() {
        DistributedPingResponse outgoing = new DistributedPingResponse(-123456789);
        byte[] bytes = outgoing.toByteArray();

        assertEquals(-123456789, outgoing.getToken());
        assertArrayEquals(new byte[] {5, 0, 0, 0, 0, (byte) 0xeb, 0x32, (byte) 0xa4, (byte) 0xf8}, bytes);
        assertEquals(-123456789, DistributedPingResponse.fromByteArray(bytes).getToken());
    }

    @Test
    @DisplayName("DistributedPingResponse defaults a missing token to zero")
    void pingResponseDefaultsMissingToken() {
        byte[] bytes = new DistributedPingRequest().toByteArray();

        assertEquals(0, DistributedPingResponse.fromByteArray(bytes).getToken());
    }

    @Test
    @DisplayName("DistributedSearchRequest preserves data and wire format")
    void searchRequestPreservesDataAndWireFormat() {
        DistributedSearchRequest outgoing = new DistributedSearchRequest("u", 0x12345678, "q");
        byte[] bytes = outgoing.toByteArray();

        assertEquals("u", outgoing.getUsername());
        assertEquals(0x12345678, outgoing.getToken());
        assertEquals("q", outgoing.getQuery());
        assertArrayEquals(
                new byte[] {19, 0, 0, 0, 3, 0, 0, 0, 0, 1, 0, 0, 0, 'u', 0x78, 0x56, 0x34, 0x12, 1, 0, 0, 0, 'q'},
                bytes);

        DistributedSearchRequest parsed = DistributedSearchRequest.fromByteArray(bytes);
        assertEquals("u", parsed.getUsername());
        assertEquals(0x12345678, parsed.getToken());
        assertEquals("q", parsed.getQuery());
    }

    @Test
    @DisplayName("DistributedSearchRequest ignores the unknown integer")
    void searchRequestIgnoresUnknownInteger() {
        byte[] bytes = new MessageBuilder()
                .writeCode(MessageCode.Distributed.SEARCH_REQUEST)
                .writeInteger(-1)
                .writeString("alice")
                .writeInteger(17)
                .writeString("music")
                .build();

        DistributedSearchRequest parsed = DistributedSearchRequest.fromByteArray(bytes);

        assertEquals("alice", parsed.getUsername());
        assertEquals(17, parsed.getToken());
        assertEquals("music", parsed.getQuery());
    }

    @Test
    @DisplayName("Distributed parsers reject mismatched message codes")
    void parsersRejectMismatchedCodes() {
        byte[] branchLevel = new DistributedBranchLevel(1).toByteArray();
        byte[] childDepth = new DistributedChildDepth(1).toByteArray();

        assertThrows(MessageException.class, () -> DistributedBranchLevel.fromByteArray(childDepth));
        assertThrows(MessageException.class, () -> DistributedBranchRoot.fromByteArray(childDepth));
        assertThrows(MessageException.class, () -> DistributedChildDepth.fromByteArray(branchLevel));
        assertThrows(MessageException.class, () -> DistributedPingRequest.fromByteArray(branchLevel));
        assertThrows(MessageException.class, () -> DistributedPingResponse.fromByteArray(branchLevel));
        assertThrows(MessageException.class, () -> DistributedSearchRequest.fromByteArray(branchLevel));
    }

    @Test
    @DisplayName("Distributed messages retain incoming and outgoing markers")
    void distributedMessagesRetainMarkers() {
        OutgoingMessage[] messages = {
            new DistributedBranchLevel(1),
            new DistributedBranchRoot("u"),
            new DistributedChildDepth(1),
            new DistributedPingRequest(),
            new DistributedPingResponse(1),
            new DistributedSearchRequest("u", 1, "q")
        };

        for (OutgoingMessage message : messages) {
            assertInstanceOf(IncomingMessage.class, message);
        }
    }
}
