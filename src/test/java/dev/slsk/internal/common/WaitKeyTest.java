// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.messaging.MessageCode;
import org.junit.jupiter.api.Test;

class WaitKeyTest {

    @Test
    void recordsRetainTypedCorrelationFields() {
        WaitKey.PeerToken key = new WaitKey.PeerToken(MessageCode.Peer.TRANSFER_RESPONSE, "alice", 42);

        assertEquals(MessageCode.Peer.TRANSFER_RESPONSE, key.code());
        assertEquals("alice", key.username());
        assertEquals(42, key.token());
    }

    @Test
    void equalKeysHaveEqualHashes() {
        WaitKey first = new WaitKey.DirectTransfer("alice", 42);
        WaitKey second = new WaitKey.DirectTransfer("alice", 42);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    void correlationKindsWithTheSameValuesAreDistinct() {
        WaitKey direct = new WaitKey.DirectTransfer("alice", 42);
        WaitKey solicited = new WaitKey.SolicitedPeer("alice", 42);

        assertNotEquals(direct, solicited);
    }

    @Test
    void fieldBoundariesCannotCollide() {
        WaitKey first = new WaitKey.PeerFile(MessageCode.Peer.TRANSFER_REQUEST, "a:b", "c");
        WaitKey second = new WaitKey.PeerFile(MessageCode.Peer.TRANSFER_REQUEST, "a", "b:c");

        assertNotEquals(first, second);
    }

    @Test
    void nullEqualityFollowsTheObjectContract() {
        WaitKey key = new WaitKey.Named("test");

        assertFalse(key.equals(null));
        assertFalse(java.util.Arrays.asList(new WaitKey[] {null}).contains(key));
    }

    @Test
    void requiredCorrelationValuesRejectNull() {
        assertThrows(NullPointerException.class, () -> new WaitKey.Named(null));
        assertThrows(NullPointerException.class, () -> new WaitKey.ServerUser(MessageCode.Server.WATCH_USER, null));
    }

    @Test
    void hierarchyIsSealed() {
        assertTrue(WaitKey.class.isSealed());
    }
}
