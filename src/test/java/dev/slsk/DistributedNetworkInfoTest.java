// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DistributedNetworkInfoTest {
    @Test
    @DisplayName("Instantiates with the given data")
    void instantiatesWithTheGivenData() {
        DistributedPeer child = new DistributedPeer("child", new InetSocketAddress("127.0.0.1", 1));
        DistributedPeer parent = new DistributedPeer("parent", new InetSocketAddress("127.0.0.1", 2));

        DistributedNetworkInfo info =
                new DistributedNetworkInfo(1.5, 3, "root", true, 4, true, List.of(child), parent, true);

        assertEquals(1.5, info.getAverageBroadcastLatency());
        assertEquals(3, info.getBranchLevel());
        assertEquals("root", info.getBranchRoot());
        assertTrue(info.isBranchRoot());
        assertEquals(4, info.getChildLimit());
        assertTrue(info.isCanAcceptChildren());
        assertEquals(List.of(child), info.getChildren());
        assertSame(parent, info.getParent());
        assertTrue(info.isHasParent());
    }

    @Test
    @DisplayName("Preserves null children and default tuple fields")
    void preservesNullChildrenAndDefaultTupleFields() {
        DistributedPeer defaultParent = new DistributedPeer(null, null);

        DistributedNetworkInfo info =
                new DistributedNetworkInfo(null, 0, null, false, 0, false, null, defaultParent, false);

        assertNull(info.getAverageBroadcastLatency());
        assertNull(info.getBranchRoot());
        assertNull(info.getChildren());
        assertFalse(info.isBranchRoot());
        assertFalse(info.isCanAcceptChildren());
        assertFalse(info.isHasParent());
        assertNull(info.getParent().username());
        assertNull(info.getParent().ipEndPoint());
    }

    @Test
    @DisplayName("Copies and protects the child list")
    void copiesAndProtectsChildList() {
        DistributedPeer child = new DistributedPeer("child", null);
        List<DistributedPeer> source = new ArrayList<>(List.of(child));
        DistributedNetworkInfo info = new DistributedNetworkInfo(
                null, 0, null, false, 0, false, source, new DistributedPeer(null, null), false);

        source.clear();

        assertEquals(List.of(child), info.getChildren());
        assertThrows(
                UnsupportedOperationException.class, () -> info.getChildren().add(child));
    }

    @Test
    @DisplayName("Rejects null tuple values that C# value tuples cannot represent")
    void rejectsNullTupleValues() {
        assertThrows(
                NullPointerException.class,
                () -> new DistributedNetworkInfo(null, 0, null, false, 0, false, null, null, false));
        assertThrows(
                NullPointerException.class,
                () -> new DistributedNetworkInfo(
                        null,
                        0,
                        null,
                        false,
                        0,
                        false,
                        java.util.Arrays.asList((DistributedPeer) null),
                        new DistributedPeer(null, null),
                        false));
    }

    @Test
    @DisplayName("DistributedPeer preserves tuple value equality")
    void distributedPeerPreservesTupleValueEquality() {
        InetSocketAddress endpoint = new InetSocketAddress("127.0.0.1", 1);

        assertEquals(new DistributedPeer("peer", endpoint), new DistributedPeer("peer", endpoint));
        assertEquals(
                new DistributedPeer("peer", endpoint).hashCode(), new DistributedPeer("peer", endpoint).hashCode());
    }
}
