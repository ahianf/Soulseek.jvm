// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-FileCopyrightText: aioslsk contributors
// SPDX-FileCopyrightText: Nicotine+ Contributors
// SPDX-License-Identifier: GPL-3.0-only

// GENERATED — edit tools/wire-vectors/ and re-run generate.py. Do not hand-edit.

package dev.slsk.internal.messaging.vectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import dev.slsk.internal.messaging.messages.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Wire vectors for the distributed message family, cross-checked against aioslsk.
 *
 * <p>5 vectors: 5 byte-exact (Tier A, encode) and
 * 0 framing-only (Tier C, decode). Tier assignment and the reason for
 * every demotion are recorded in tools/wire-vectors/bindings.json.
 */
class DistributedVectorsTest {
    private static byte[] hex(String value) {
        return WireVectors.hex(value);
    }

    @Nested
    @DisplayName("DistributedBranchLevel")
    class DistributedBranchLevelVectors {

        @Test
        @DisplayName("test_DistributedBranchLevel_Request_deserialize, test_DistributedBranchLevel_Request_serialize,"
                + " test_whenDeserializeDistributedRequest_shouldDeserialize")
        void distributedBranchLevel_Request_deserialize() {
            assertArrayEquals(hex("050000000405000000"), new DistributedBranchLevel(5).toByteArray());
        }
    }

    @Nested
    @DisplayName("DistributedBranchRoot")
    class DistributedBranchRootVectors {

        @Test
        @DisplayName("test_DistributedBranchRoot_Request_deserialize, test_DistributedBranchRoot_Request_serialize")
        void distributedBranchRoot_Request_deserialize() {
            assertArrayEquals(hex("0a00000005050000007573657230"), new DistributedBranchRoot("user0").toByteArray());
        }
    }

    @Nested
    @DisplayName("DistributedChildDepth")
    class DistributedChildDepthVectors {

        @Test
        @DisplayName("test_DistributedChildDepth_Request_deserialize, test_DistributedChildDepth_Request_serialize")
        void distributedChildDepth_Request_deserialize() {
            assertArrayEquals(hex("050000000705000000"), new DistributedChildDepth(5).toByteArray());
        }
    }

    @Nested
    @DisplayName("DistributedPing")
    class DistributedPingVectors {

        @Test
        @DisplayName("test_DistributedPing_Request_deserialize, test_DistributedPing_Request_serialize")
        void distributedPing_Request_deserialize() {
            assertArrayEquals(hex("0100000000"), new DistributedPingRequest().toByteArray());
        }
    }

    @Nested
    @DisplayName("DistributedSearchRequest")
    class DistributedSearchRequestVectors {

        @Test
        @DisplayName(
                "test_DistributedSearchRequest_Request_deserialize, test_DistributedSearchRequest_Request_serialize")
        void distributedSearchRequest_Request_deserialize() {
            assertArrayEquals(
                    hex("1b0000000331000000050000007573657230d2040000050000005175657279"),
                    new DistributedSearchRequest("user0", 1234, "Query").toByteArray());
        }
    }
}
