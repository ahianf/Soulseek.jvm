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

class PeerRequestMessageTest {
    @Test
    @DisplayName("BrowseRequestMessage has the source empty peer-message vector")
    void browseRequestHasWireVector() {
        assertArrayEquals(new byte[] {4, 0, 0, 0, 4, 0, 0, 0}, new BrowseRequestMessage().toByteArray());
    }

    @Test
    @DisplayName("UserInfoRequest has the source empty peer-message vector")
    void userInfoRequestHasWireVector() {
        assertArrayEquals(new byte[] {4, 0, 0, 0, 15, 0, 0, 0}, new UserInfoRequest().toByteArray());
    }

    @Test
    @DisplayName("FolderContentsRequest preserves data and wire format")
    void folderContentsRequestPreservesDataAndWireFormat() {
        FolderContentsRequest outgoing = new FolderContentsRequest(0x12345678, "d");
        byte[] bytes = outgoing.toByteArray();

        assertEquals(0x12345678, outgoing.getToken());
        assertEquals("d", outgoing.getDirectoryName());
        assertArrayEquals(new byte[] {13, 0, 0, 0, 36, 0, 0, 0, 0x78, 0x56, 0x34, 0x12, 1, 0, 0, 0, 'd'}, bytes);

        FolderContentsRequest parsed = FolderContentsRequest.fromByteArray(bytes);
        assertEquals(0x12345678, parsed.getToken());
        assertEquals("d", parsed.getDirectoryName());
    }

    @Test
    @DisplayName("PeerSearchRequest preserves constructor and parsed data")
    void peerSearchRequestPreservesData() {
        PeerSearchRequest direct = new PeerSearchRequest(-17, "query");
        byte[] bytes = new MessageBuilder()
                .writeCode(MessageCode.Peer.SEARCH_REQUEST)
                .writeInteger(-17)
                .writeString("query")
                .build();
        PeerSearchRequest parsed = PeerSearchRequest.fromByteArray(bytes);

        assertEquals(-17, direct.getToken());
        assertEquals("query", direct.getQuery());
        assertEquals(-17, parsed.getToken());
        assertEquals("query", parsed.getQuery());
        assertInstanceOf(IncomingMessage.class, direct);
    }

    @Test
    @DisplayName("PlaceInQueueRequest preserves data and wire format")
    void placeInQueueRequestPreservesDataAndWireFormat() {
        PlaceInQueueRequest outgoing = new PlaceInQueueRequest("f");
        byte[] bytes = outgoing.toByteArray();

        assertEquals("f", outgoing.getFilename());
        assertArrayEquals(new byte[] {9, 0, 0, 0, 51, 0, 0, 0, 1, 0, 0, 0, 'f'}, bytes);
        assertEquals("f", PlaceInQueueRequest.fromByteArray(bytes).getFilename());
    }

    @Test
    @DisplayName("PlaceInQueueResponse preserves data and wire format")
    void placeInQueueResponsePreservesDataAndWireFormat() {
        PlaceInQueueResponse outgoing = new PlaceInQueueResponse("f", Integer.MIN_VALUE);
        byte[] bytes = outgoing.toByteArray();

        assertEquals("f", outgoing.getFilename());
        assertEquals(Integer.MIN_VALUE, outgoing.getPlaceInQueue());
        assertArrayEquals(new byte[] {13, 0, 0, 0, 44, 0, 0, 0, 1, 0, 0, 0, 'f', 0, 0, 0, (byte) 0x80}, bytes);
        PlaceInQueueResponse parsed = PlaceInQueueResponse.fromByteArray(bytes);
        assertEquals("f", parsed.getFilename());
        assertEquals(Integer.MIN_VALUE, parsed.getPlaceInQueue());
    }

    @Test
    @DisplayName("QueueDownloadRequest preserves data and wire format")
    void queueDownloadRequestPreservesDataAndWireFormat() {
        QueueDownloadRequest outgoing = new QueueDownloadRequest("f");
        byte[] bytes = outgoing.toByteArray();

        assertEquals("f", outgoing.getFilename());
        assertArrayEquals(new byte[] {9, 0, 0, 0, 43, 0, 0, 0, 1, 0, 0, 0, 'f'}, bytes);
        assertEquals("f", QueueDownloadRequest.fromByteArray(bytes).getFilename());
    }

    @Test
    @DisplayName("Peer request parsers reject mismatched message codes")
    void parsersRejectMismatchedCodes() {
        byte[] browse = new BrowseRequestMessage().toByteArray();

        assertThrows(MessageException.class, () -> FolderContentsRequest.fromByteArray(browse));
        assertThrows(MessageException.class, () -> PeerSearchRequest.fromByteArray(browse));
        assertThrows(MessageException.class, () -> PlaceInQueueRequest.fromByteArray(browse));
        assertThrows(MessageException.class, () -> PlaceInQueueResponse.fromByteArray(browse));
        assertThrows(MessageException.class, () -> QueueDownloadRequest.fromByteArray(browse));
    }

    @Test
    @DisplayName("Peer request parsers preserve missing-data failures")
    void parsersPreserveMissingDataFailures() {
        assertThrows(
                MessageReadException.class,
                () -> FolderContentsRequest.fromByteArray(empty(MessageCode.Peer.FOLDER_CONTENTS_REQUEST)));
        assertThrows(
                MessageReadException.class,
                () -> PeerSearchRequest.fromByteArray(empty(MessageCode.Peer.SEARCH_REQUEST)));
        assertThrows(
                MessageReadException.class,
                () -> PlaceInQueueRequest.fromByteArray(empty(MessageCode.Peer.PLACE_IN_QUEUE_REQUEST)));
        assertThrows(
                MessageReadException.class,
                () -> PlaceInQueueResponse.fromByteArray(empty(MessageCode.Peer.PLACE_IN_QUEUE_RESPONSE)));
        assertThrows(
                MessageReadException.class,
                () -> QueueDownloadRequest.fromByteArray(empty(MessageCode.Peer.QUEUE_DOWNLOAD)));
    }

    @Test
    @DisplayName("Bidirectional peer messages retain both markers")
    void bidirectionalMessagesRetainMarkers() {
        OutgoingMessage[] messages = {
            new FolderContentsRequest(1, "d"),
            new PlaceInQueueRequest("f"),
            new PlaceInQueueResponse("f", 1),
            new QueueDownloadRequest("f")
        };

        for (OutgoingMessage message : messages) {
            assertInstanceOf(IncomingMessage.class, message);
        }
    }

    private static byte[] empty(MessageCode.Peer code) {
        return new MessageBuilder().writeCode(code).build();
    }
}
