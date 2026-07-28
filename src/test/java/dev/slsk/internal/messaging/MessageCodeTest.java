// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MessageCodeTest {
    @Test
    @DisplayName("Distributed codes preserve every source value")
    void distributedCodesPreserveValues() {
        assertCodes(
                MessageCode.Distributed.values(),
                Map.of(
                        "PING", 0,
                        "SEARCH_REQUEST", 3,
                        "BRANCH_LEVEL", 4,
                        "BRANCH_ROOT", 5,
                        "UNKNOWN", 6,
                        "CHILD_DEPTH", 7,
                        "EMBEDDED_MESSAGE", 93),
                1);
    }

    @Test
    @DisplayName("Initialization codes preserve every source value")
    void initializationCodesPreserveValues() {
        assertCodes(MessageCode.Initialization.values(), Map.of("PIERCE_FIREWALL", 0, "PEER_INIT", 1), 1);
    }

    @Test
    @DisplayName("Peer codes preserve every source value")
    void peerCodesPreserveValues() {
        Map<String, Integer> expected = new LinkedHashMap<>();
        expected.put("PRIVATE_MESSAGE", 1);
        expected.put("BROWSE_REQUEST", 4);
        expected.put("BROWSE_RESPONSE", 5);
        expected.put("SEARCH_REQUEST", 8);
        expected.put("SEARCH_RESPONSE", 9);
        expected.put("PRIVATE_ROOM_INVITATION", 10);
        expected.put("CANCELLED_QUEUED_TRANSFER", 14);
        expected.put("INFO_REQUEST", 15);
        expected.put("INFO_RESPONSE", 16);
        expected.put("SEND_CONNECT_TOKEN", 33);
        expected.put("MOVE_DOWNLOAD_TO_TOP", 34);
        expected.put("FOLDER_CONTENTS_REQUEST", 36);
        expected.put("FOLDER_CONTENTS_RESPONSE", 37);
        expected.put("TRANSFER_REQUEST", 40);
        expected.put("TRANSFER_RESPONSE", 41);
        expected.put("UPLOAD_PLACEHOLD", 42);
        expected.put("QUEUE_DOWNLOAD", 43);
        expected.put("PLACE_IN_QUEUE_RESPONSE", 44);
        expected.put("UPLOAD_FAILED", 46);
        expected.put("EXACT_FILE_SEARCH_REQUEST", 47);
        expected.put("QUEUED_DOWNLOADS", 48);
        expected.put("INDIRECT_FILE_SEARCH_REQUEST", 49);
        expected.put("UPLOAD_DENIED", 50);
        expected.put("PLACE_IN_QUEUE_REQUEST", 51);
        expected.put("UPLOAD_QUEUE_NOTIFICATION", 52);

        assertCodes(MessageCode.Peer.values(), expected, 4);
    }

    @Test
    @DisplayName("Server codes preserve every source value")
    void serverCodesPreserveValues() {
        int[] values = {
            0, 1, 2, 3, 5, 6, 7, 13, 14, 15, 16, 17, 18, 22, 23, 26, 28, 32, 34, 35, 36, 40, 41, 42, 51, 52, 54, 56, 57,
            64, 65, 66, 69, 71, 73, 83, 84, 86, 87, 88, 90, 91, 92, 93, 100, 102, 103, 104, 110, 111, 112, 113, 114,
            115, 116, 117, 118, 120, 121, 122, 123, 124, 125, 126, 127, 129, 130, 133, 134, 135, 136, 137, 138, 139,
            140, 141, 142, 143, 144, 145, 146, 148, 149, 150, 151, 152, 153, 160, 1001, 1002, 1003
        };
        MessageCode.Server[] codes = MessageCode.Server.values();

        assertEquals(values.length, codes.length);
        for (int index = 0; index < values.length; index++) {
            assertEquals(values[index], codes[index].getValue());
            assertEquals(4, codes[index].getByteLength());
            assertEquals(codes[index], MessageCode.Server.fromValue(values[index]));
        }
    }

    @Test
    @DisplayName("Unknown code values are rejected")
    void unknownValuesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> MessageCode.Distributed.fromValue(2));
        assertThrows(IllegalArgumentException.class, () -> MessageCode.Initialization.fromValue(2));
        assertThrows(IllegalArgumentException.class, () -> MessageCode.Peer.fromValue(2));
        assertThrows(IllegalArgumentException.class, () -> MessageCode.Server.fromValue(4));
    }

    private static <T extends Enum<T> & ProtocolCode> void assertCodes(
            T[] codes, Map<String, Integer> expected, int byteLength) {
        assertEquals(expected.size(), codes.length);
        for (T code : codes) {
            assertEquals(expected.get(code.name()), code.getValue());
            assertEquals(byteLength, code.getByteLength());
        }
    }
}
