// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DistributedEventTest {
    @Test
    @DisplayName("DistributedParentEvent instantiates properly")
    void parentInstantiatesProperly() {
        InetSocketAddress endpoint = InetSocketAddress.createUnresolved("example.test", 2242);
        DistributedParentEvent args = new DistributedParentEvent("alice", endpoint, 2, "root");

        assertEquals("alice", args.getUsername());
        assertSame(endpoint, args.getIpEndpoint());
        assertEquals(2, args.getBranchLevel());
        assertEquals("root", args.getBranchRoot());
        assertFalse(args.isBranchRoot());
    }

    @Test
    @DisplayName("DistributedParentEvent detects a branch root")
    void parentDetectsBranchRoot() {
        String username = new String("alice");
        String branchRoot = new String("alice");
        DistributedParentEvent args = new DistributedParentEvent(username, null, 0, branchRoot);

        assertTrue(args.isBranchRoot());
    }

    @Test
    @DisplayName("DistributedParentEvent preserves C# null equality")
    void parentPreservesNullEquality() {
        DistributedParentEvent root = new DistributedParentEvent(null, null, 0, null);
        DistributedParentEvent child = new DistributedParentEvent(null, null, 1, null);

        assertTrue(root.isBranchRoot());
        assertFalse(child.isBranchRoot());
        assertNull(root.getUsername());
        assertNull(root.getBranchRoot());
        assertNull(root.getIpEndpoint());
    }

    @Test
    @DisplayName("DistributedChildEvent instantiates properly")
    void childInstantiatesProperly() {
        InetSocketAddress endpoint = InetSocketAddress.createUnresolved("example.test", 2242);
        DistributedChildEvent args = new DistributedChildEvent("alice", endpoint);

        assertEquals("alice", args.getUsername());
        assertSame(endpoint, args.getIpEndpoint());
    }
}
