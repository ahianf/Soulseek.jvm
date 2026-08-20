// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.transfer.TransferState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FlagStatesTest {
    @Test
    @DisplayName("Client lifecycle exposes connected and authenticated phases")
    void clientLifecycleExposesSemanticPhases() {
        SoulseekClientState state = SoulseekClientState.LOGGED_IN;

        assertTrue(state.isConnected());
        assertTrue(state.isLoggedIn());
        assertTrue(SoulseekClientState.CONNECTED.isConnected());
        assertFalse(SoulseekClientState.CONNECTED.isLoggedIn());
        assertFalse(SoulseekClientState.CONNECTING.isConnected());
    }

    @Test
    @DisplayName("Combines and tests transfer state flags")
    void combinesAndTestsTransferStateFlags() {
        TransferState state = TransferState.QUEUED.or(TransferState.REMOTELY);

        assertEquals(4098, state.getValue());
        assertTrue(state.contains(TransferState.QUEUED));
        assertTrue(state.contains(TransferState.REMOTELY));
        assertFalse(state.contains(TransferState.LOCALLY));
        assertEquals(state, TransferState.fromValue(4098));
        assertEquals("QUEUED | REMOTELY", state.toString());
    }

    @Test
    @DisplayName("Preserves all declared transfer state values")
    void preservesTransferStateValues() {
        assertEquals(0, TransferState.NONE.getValue());
        assertEquals(1, TransferState.REQUESTED.getValue());
        assertEquals(2, TransferState.QUEUED.getValue());
        assertEquals(4, TransferState.INITIALIZING.getValue());
        assertEquals(8, TransferState.IN_PROGRESS.getValue());
        assertEquals(16, TransferState.COMPLETED.getValue());
        assertEquals(32, TransferState.SUCCEEDED.getValue());
        assertEquals(64, TransferState.CANCELLED.getValue());
        assertEquals(128, TransferState.TIMED_OUT.getValue());
        assertEquals(256, TransferState.ERRORED.getValue());
        assertEquals(512, TransferState.REJECTED.getValue());
        assertEquals(1024, TransferState.ABORTED.getValue());
        assertEquals(2048, TransferState.LOCALLY.getValue());
        assertEquals(4096, TransferState.REMOTELY.getValue());
    }

    @Test
    @DisplayName("Returns canonical instances for declared single states")
    void returnsCanonicalInstancesForDeclaredStates() {
        assertSame(TransferState.REJECTED, TransferState.fromValue(512));
    }

    @Test
    @DisplayName("None is present in every flags value like Enum.HasFlag")
    void noneIsPresentInEveryFlagsValue() {
        assertTrue(TransferState.IN_PROGRESS.contains(TransferState.NONE));
    }
}
