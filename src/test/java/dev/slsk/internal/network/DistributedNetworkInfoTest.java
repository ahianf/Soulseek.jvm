// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

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

        assertEquals(1.5, info.averageBroadcastLatency());
        assertEquals(3, info.branchLevel());
        assertEquals("root", info.branchRoot());
        assertTrue(info.branchRootNode());
        assertEquals(4, info.childLimit());
        assertTrue(info.acceptChildren());
        assertEquals(List.of(child), info.children());
        assertSame(parent, info.parent());
        assertTrue(info.parentPresent());
    }

    @Test
    @DisplayName("Preserves null children and default tuple fields")
    void preservesNullChildrenAndDefaultTupleFields() {
        DistributedPeer defaultParent = new DistributedPeer(null, null);

        DistributedNetworkInfo info =
                new DistributedNetworkInfo(null, 0, null, false, 0, false, null, defaultParent, false);

        assertNull(info.averageBroadcastLatency());
        assertNull(info.branchRoot());
        assertNull(info.children());
        assertFalse(info.branchRootNode());
        assertFalse(info.acceptChildren());
        assertFalse(info.parentPresent());
        assertNull(info.parent().username());
        assertNull(info.parent().ipEndpoint());
    }

    @Test
    @DisplayName("Copies and protects the child list")
    void copiesAndProtectsChildList() {
        DistributedPeer child = new DistributedPeer("child", null);
        List<DistributedPeer> source = new ArrayList<>(List.of(child));
        DistributedNetworkInfo info = new DistributedNetworkInfo(
                null, 0, null, false, 0, false, source, new DistributedPeer(null, null), false);

        source.clear();

        assertEquals(List.of(child), info.children());
        assertThrows(UnsupportedOperationException.class, () -> info.children().add(child));
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
