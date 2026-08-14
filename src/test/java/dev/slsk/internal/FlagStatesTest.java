// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.search.SearchState;
import dev.slsk.internal.transfer.TransferState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FlagStatesTest {
    @Test
    @DisplayName("Combines and tests client state flags")
    void combinesAndTestsClientStateFlags() {
        SoulseekClientState state = SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGED_IN);

        assertEquals(6, state.getValue());
        assertTrue(state.contains(SoulseekClientState.CONNECTED));
        assertTrue(state.contains(SoulseekClientState.LOGGED_IN));
        assertFalse(state.contains(SoulseekClientState.CONNECTING));
        assertEquals(state, SoulseekClientState.fromValue(6));
        assertEquals("CONNECTED | LOGGED_IN", state.toString());
    }

    @Test
    @DisplayName("Combines and tests search state flags")
    void combinesAndTestsSearchStateFlags() {
        SearchState state = SearchState.COMPLETED.or(SearchState.TIMED_OUT);

        assertEquals(20, state.getValue());
        assertTrue(state.contains(SearchState.COMPLETED));
        assertTrue(state.contains(SearchState.TIMED_OUT));
        assertFalse(state.contains(SearchState.CANCELLED));
        assertEquals(state, SearchState.fromValue(20));
        assertEquals("COMPLETED | TIMED_OUT", state.toString());
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
        assertSame(SoulseekClientState.CONNECTED, SoulseekClientState.fromValue(2));
        assertSame(SearchState.QUEUED, SearchState.fromValue(256));
        assertSame(TransferState.REJECTED, TransferState.fromValue(512));
    }

    @Test
    @DisplayName("Uses value equality for combined states")
    void usesValueEqualityForCombinedStates() {
        SearchState first = SearchState.COMPLETED.or(SearchState.ERRORED);
        SearchState second = SearchState.fromValue(132);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertNotEquals(first, SearchState.COMPLETED);
    }

    @Test
    @DisplayName("None is present in every flags value like Enum.HasFlag")
    void noneIsPresentInEveryFlagsValue() {
        assertTrue(SoulseekClientState.CONNECTED.contains(SoulseekClientState.NONE));
        assertTrue(SearchState.COMPLETED.contains(SearchState.NONE));
        assertTrue(TransferState.IN_PROGRESS.contains(TransferState.NONE));
    }
}
