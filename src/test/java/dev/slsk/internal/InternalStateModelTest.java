// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.transfer.TransferPhase;
import dev.slsk.internal.transfer.TransferQueueLocation;
import dev.slsk.internal.transfer.TransferTermination;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class InternalStateModelTest {
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
    @DisplayName("Transfer phase, queue location, and termination are separate concepts")
    void transferStateConceptsAreSeparate() {
        assertNotEquals(TransferPhase.QUEUED.name(), TransferQueueLocation.REMOTE.name());
        assertNotEquals(TransferPhase.COMPLETED.name(), TransferTermination.SUCCEEDED.name());
        assertTrue(TransferPhase.class.isEnum());
        assertTrue(TransferQueueLocation.class.isEnum());
        assertTrue(TransferTermination.class.isEnum());
    }
}
