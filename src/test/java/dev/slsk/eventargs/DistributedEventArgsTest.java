// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DistributedEventArgsTest {
    @Test
    @DisplayName("DistributedParentEventArgs instantiates properly")
    void parentInstantiatesProperly() {
        InetSocketAddress endpoint = InetSocketAddress.createUnresolved("example.test", 2242);
        DistributedParentEventArgs args = new DistributedParentEventArgs("alice", endpoint, 2, "root");

        assertEquals("alice", args.getUsername());
        assertSame(endpoint, args.getIpEndPoint());
        assertEquals(2, args.getBranchLevel());
        assertEquals("root", args.getBranchRoot());
        assertFalse(args.isBranchRoot());
    }

    @Test
    @DisplayName("DistributedParentEventArgs detects a branch root")
    void parentDetectsBranchRoot() {
        String username = new String("alice");
        String branchRoot = new String("alice");
        DistributedParentEventArgs args = new DistributedParentEventArgs(username, null, 0, branchRoot);

        assertTrue(args.isBranchRoot());
    }

    @Test
    @DisplayName("DistributedParentEventArgs preserves C# null equality")
    void parentPreservesNullEquality() {
        DistributedParentEventArgs root = new DistributedParentEventArgs(null, null, 0, null);
        DistributedParentEventArgs child = new DistributedParentEventArgs(null, null, 1, null);

        assertTrue(root.isBranchRoot());
        assertFalse(child.isBranchRoot());
        assertNull(root.getUsername());
        assertNull(root.getBranchRoot());
        assertNull(root.getIpEndPoint());
    }

    @Test
    @DisplayName("DistributedChildEventArgs instantiates properly")
    void childInstantiatesProperly() {
        InetSocketAddress endpoint = InetSocketAddress.createUnresolved("example.test", 2242);
        DistributedChildEventArgs args = new DistributedChildEventArgs("alice", endpoint);

        assertEquals("alice", args.getUsername());
        assertSame(endpoint, args.getIpEndPoint());
    }
}
