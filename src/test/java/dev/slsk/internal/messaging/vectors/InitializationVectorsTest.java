// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-FileCopyrightText: aioslsk contributors
// SPDX-FileCopyrightText: Nicotine+ Contributors
// SPDX-License-Identifier: GPL-3.0-only

// GENERATED — edit tools/wire-vectors/ and re-run generate.py. Do not hand-edit.

package dev.slsk.internal.messaging.vectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import dev.slsk.internal.messaging.messages.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Wire vectors for the peer_init message family, cross-checked against aioslsk.
 *
 * <p>3 vectors: 2 byte-exact (Tier A, encode) and
 * 1 framing-only (Tier C, decode). Tier assignment and the reason for
 * every demotion are recorded in tools/wire-vectors/bindings.json.
 */
class InitializationVectorsTest {
    private static byte[] hex(String value) {
        return WireVectors.hex(value);
    }

    @Nested
    @DisplayName("PeerInit")
    class PeerInitVectors {

        @Test
        @DisplayName("test_PeerInit_Request_deserialize_uint32, test_PeerInit_Request_serialize")
        void peerInit_Request_deserialize_uint32() {
            assertArrayEquals(
                    hex("13000000010500000075736572300100000044e8030000"),
                    new PeerInit("user0", "D", 1000).toByteArray());
        }

        @Test
        @DisplayName("test_PeerInit_Request_deserialize_uint64")
        void peerInit_Request_deserialize_uint64_decodes() {
            assertNotNull(assertDoesNotThrow(
                    () -> PeerInit.tryFromByteArray(hex("13000000010500000075736572300100000044e803000000000000"))
                            .orElseThrow()));
        }
    }

    @Nested
    @DisplayName("PeerPierceFirewall")
    class PeerPierceFirewallVectors {

        @Test
        @DisplayName("test_PeerPierceFirewall_Request_deserialize, test_PeerPierceFirewall_Request_serialize,"
                + " test_whenDeserializePeerInitializationRequest_shouldDeserialize")
        void peerPierceFirewall_Request_deserialize() {
            assertArrayEquals(hex("0500000000e8030000"), new PierceFirewall(1000).toByteArray());
        }
    }
}
