// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ConstantsTest {
    @Test
    void preservesEverySourceConstant() {
        assertEquals(170, Constants.MAJOR_VERSION);
        assertEquals("Direct", Constants.ConnectionMethod.DIRECT);
        assertEquals("Indirect", Constants.ConnectionMethod.INDIRECT);
        assertEquals("D", Constants.ConnectionType.DISTRIBUTED);
        assertEquals("P", Constants.ConnectionType.PEER);
        assertEquals("F", Constants.ConnectionType.TRANSFER);
        assertEquals("BranchLevelMessage", Constants.WaitKey.BRANCH_LEVEL_MESSAGE);
        assertEquals("BranchRootMessage", Constants.WaitKey.BRANCH_ROOT_MESSAGE);
        assertEquals("BrowseResponseConnection", Constants.WaitKey.BROWSE_RESPONSE_CONNECTION);
        assertEquals("ChildDepthMessage", Constants.WaitKey.CHILD_DEPTH_MESSAGE);
        assertEquals("DirectTransfer", Constants.WaitKey.DIRECT_TRANSFER);
        assertEquals("IndirectTransfer", Constants.WaitKey.INDIRECT_TRANSFER);
        assertEquals("SearchRequestMessage", Constants.WaitKey.SEARCH_REQUEST_MESSAGE);
        assertEquals("SolicitedDistributedConnection", Constants.WaitKey.SOLICITED_DISTRIBUTED_CONNECTION);
        assertEquals("SolicitedPeerConnection", Constants.WaitKey.SOLICITED_PEER_CONNECTION);
        assertEquals("Transfer", Constants.WaitKey.TRANSFER);
    }
}
