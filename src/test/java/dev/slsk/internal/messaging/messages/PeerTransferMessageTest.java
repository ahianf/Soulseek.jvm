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
import dev.slsk.internal.TransferDirection;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PeerTransferMessageTest {
    @Test
    @DisplayName("TransferRequest preserves constructor defaults and data")
    void transferRequestPreservesConstructorData() {
        TransferRequest defaults = new TransferRequest(TransferDirection.DOWNLOAD, 7, "f");
        TransferRequest full = new TransferRequest(TransferDirection.UPLOAD, -7, "g", -9);

        assertEquals(TransferDirection.DOWNLOAD, defaults.getDirection());
        assertEquals(7, defaults.getToken());
        assertEquals("f", defaults.getFilename());
        assertEquals(0, defaults.getFileSize());
        assertEquals(TransferDirection.UPLOAD, full.getDirection());
        assertEquals(-7, full.getToken());
        assertEquals("g", full.getFilename());
        assertEquals(-9, full.getFileSize());
        assertThrows(NullPointerException.class, () -> new TransferRequest(null, 1, "f"));
    }

    @Test
    @DisplayName("TransferRequest preserves its exact wire format")
    void transferRequestPreservesWireFormat() {
        byte[] bytes =
                new TransferRequest(TransferDirection.UPLOAD, 0x12345678, "f", 0x0102030405060708L).toByteArray();

        assertArrayEquals(
                new byte[] {
                    25, 0, 0, 0, 40, 0, 0, 0, 1, 0, 0, 0, 0x78, 0x56, 0x34, 0x12, 1, 0, 0, 0, 'f', 8, 7, 6, 5, 4, 3, 2,
                    1
                },
                bytes);

        TransferRequest parsed = TransferRequest.fromByteArray(bytes);
        assertEquals(TransferDirection.UPLOAD, parsed.getDirection());
        assertEquals(0x12345678, parsed.getToken());
        assertEquals("f", parsed.getFilename());
        assertEquals(0x0102030405060708L, parsed.getFileSize());
    }

    @Test
    @DisplayName("TransferRequest defaults a missing file size to zero")
    void transferRequestDefaultsMissingFileSize() {
        byte[] bytes = new MessageBuilder()
                .writeCode(MessageCode.Peer.TRANSFER_REQUEST)
                .writeInteger(TransferDirection.DOWNLOAD.getValue())
                .writeInteger(17)
                .writeString("f")
                .build();

        TransferRequest parsed = TransferRequest.fromByteArray(bytes);

        assertEquals(TransferDirection.DOWNLOAD, parsed.getDirection());
        assertEquals(17, parsed.getToken());
        assertEquals("f", parsed.getFilename());
        assertEquals(0, parsed.getFileSize());
    }

    @Test
    @DisplayName("TransferResponse constructors preserve all three branches")
    void transferResponseConstructorsPreserveBranches() {
        TransferResponse denied = new TransferResponse(1, "no");
        TransferResponse sized = new TransferResponse(2, 99L);
        TransferResponse unsized = new TransferResponse(3);

        assertFalse(denied.isAllowed());
        assertEquals(1, denied.getToken());
        assertEquals("no", denied.getMessage());
        assertEquals(0, denied.getFileSize());
        assertTrue(sized.isAllowed());
        assertEquals(2, sized.getToken());
        assertEquals(99, sized.getFileSize());
        assertNull(sized.getMessage());
        assertTrue(unsized.isAllowed());
        assertEquals(3, unsized.getToken());
        assertEquals(0, unsized.getFileSize());
        assertNull(unsized.getMessage());
    }

    @Test
    @DisplayName("Allowed TransferResponse preserves its exact wire format")
    void allowedTransferResponsePreservesWireFormat() {
        byte[] bytes = new TransferResponse(0x12345678, 0x0102030405060708L).toByteArray();

        assertArrayEquals(
                new byte[] {17, 0, 0, 0, 41, 0, 0, 0, 0x78, 0x56, 0x34, 0x12, 1, 8, 7, 6, 5, 4, 3, 2, 1}, bytes);
        TransferResponse parsed = TransferResponse.fromByteArray(bytes);
        assertTrue(parsed.isAllowed());
        assertEquals(0x12345678, parsed.getToken());
        assertEquals(0x0102030405060708L, parsed.getFileSize());
        assertNull(parsed.getMessage());
    }

    @Test
    @DisplayName("Denied TransferResponse preserves its exact wire format")
    void deniedTransferResponsePreservesWireFormat() {
        byte[] bytes = new TransferResponse(0x12345678, "no").toByteArray();

        assertArrayEquals(
                new byte[] {15, 0, 0, 0, 41, 0, 0, 0, 0x78, 0x56, 0x34, 0x12, 0, 2, 0, 0, 0, 'n', 'o'}, bytes);
        TransferResponse parsed = TransferResponse.fromByteArray(bytes);
        assertFalse(parsed.isAllowed());
        assertEquals(0x12345678, parsed.getToken());
        assertEquals("no", parsed.getMessage());
        assertEquals(0, parsed.getFileSize());
    }

    @Test
    @DisplayName("TransferResponse accepts an allowed response without size")
    void transferResponseAcceptsAllowedResponseWithoutSize() {
        byte[] bytes = new MessageBuilder()
                .writeCode(MessageCode.Peer.TRANSFER_RESPONSE)
                .writeInteger(17)
                .writeByte(1)
                .build();

        TransferResponse parsed = TransferResponse.fromByteArray(bytes);

        assertTrue(parsed.isAllowed());
        assertEquals(17, parsed.getToken());
        assertEquals(0, parsed.getFileSize());
    }

    @Test
    @DisplayName("TransferResponse treats only byte one as allowed")
    void transferResponseTreatsOnlyOneAsAllowed() {
        byte[] bytes = new MessageBuilder()
                .writeCode(MessageCode.Peer.TRANSFER_RESPONSE)
                .writeInteger(17)
                .writeByte(2)
                .writeString("no")
                .build();

        TransferResponse parsed = TransferResponse.fromByteArray(bytes);

        assertFalse(parsed.isAllowed());
        assertEquals("no", parsed.getMessage());
    }

    @Test
    @DisplayName("UploadDenied preserves data and exact wire format")
    void uploadDeniedPreservesDataAndWireFormat() {
        UploadDenied outgoing = new UploadDenied("f", "no");
        byte[] bytes = outgoing.toByteArray();

        assertEquals("f", outgoing.getFilename());
        assertEquals("no", outgoing.getMessage());
        assertArrayEquals(new byte[] {15, 0, 0, 0, 50, 0, 0, 0, 1, 0, 0, 0, 'f', 2, 0, 0, 0, 'n', 'o'}, bytes);
        UploadDenied parsed = UploadDenied.fromByteArray(bytes);
        assertEquals("f", parsed.getFilename());
        assertEquals("no", parsed.getMessage());
    }

    @Test
    @DisplayName("UploadFailed preserves data and exact wire format")
    void uploadFailedPreservesDataAndWireFormat() {
        UploadFailed outgoing = new UploadFailed("f");
        byte[] bytes = outgoing.toByteArray();

        assertEquals("f", outgoing.getFilename());
        assertArrayEquals(new byte[] {9, 0, 0, 0, 46, 0, 0, 0, 1, 0, 0, 0, 'f'}, bytes);
        assertEquals("f", UploadFailed.fromByteArray(bytes).getFilename());
    }

    @Test
    @DisplayName("Peer transfer parsers reject mismatches and missing data")
    void peerTransferParsersRejectInvalidData() {
        byte[] browse = new BrowseRequest().toByteArray();

        assertThrows(MessageException.class, () -> TransferRequest.fromByteArray(browse));
        assertThrows(MessageException.class, () -> TransferResponse.fromByteArray(browse));
        assertThrows(MessageException.class, () -> UploadDenied.fromByteArray(browse));
        assertThrows(MessageException.class, () -> UploadFailed.fromByteArray(browse));
        assertThrows(
                MessageReadException.class,
                () -> TransferRequest.fromByteArray(empty(MessageCode.Peer.TRANSFER_REQUEST)));
        assertThrows(
                MessageReadException.class,
                () -> TransferResponse.fromByteArray(empty(MessageCode.Peer.TRANSFER_RESPONSE)));
        assertThrows(
                MessageReadException.class, () -> UploadDenied.fromByteArray(empty(MessageCode.Peer.UPLOAD_DENIED)));
        assertThrows(
                MessageReadException.class, () -> UploadFailed.fromByteArray(empty(MessageCode.Peer.UPLOAD_FAILED)));
    }

    private static byte[] empty(MessageCode.Peer code) {
        return new MessageBuilder().writeCode(code).build();
    }
}
