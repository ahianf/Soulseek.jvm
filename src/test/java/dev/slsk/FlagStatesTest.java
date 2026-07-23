// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FlagStatesTest {
    @Test
    @DisplayName("Combines and tests client state flags")
    void combinesAndTestsClientStateFlags() {
        SoulseekClientStates state = SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGED_IN);

        assertEquals(6, state.getValue());
        assertTrue(state.hasFlag(SoulseekClientStates.CONNECTED));
        assertTrue(state.hasFlag(SoulseekClientStates.LOGGED_IN));
        assertFalse(state.hasFlag(SoulseekClientStates.CONNECTING));
        assertEquals(state, SoulseekClientStates.fromValue(6));
        assertEquals("CONNECTED | LOGGED_IN", state.toString());
    }

    @Test
    @DisplayName("Combines and tests search state flags")
    void combinesAndTestsSearchStateFlags() {
        SearchStates state = SearchStates.COMPLETED.or(SearchStates.TIMED_OUT);

        assertEquals(20, state.getValue());
        assertTrue(state.hasFlag(SearchStates.COMPLETED));
        assertTrue(state.hasFlag(SearchStates.TIMED_OUT));
        assertFalse(state.hasFlag(SearchStates.CANCELLED));
        assertEquals(state, SearchStates.fromValue(20));
        assertEquals("COMPLETED | TIMED_OUT", state.toString());
    }

    @Test
    @DisplayName("Combines and tests transfer state flags")
    void combinesAndTestsTransferStateFlags() {
        TransferStates state = TransferStates.QUEUED.or(TransferStates.REMOTELY);

        assertEquals(4098, state.getValue());
        assertTrue(state.hasFlag(TransferStates.QUEUED));
        assertTrue(state.hasFlag(TransferStates.REMOTELY));
        assertFalse(state.hasFlag(TransferStates.LOCALLY));
        assertEquals(state, TransferStates.fromValue(4098));
        assertEquals("QUEUED | REMOTELY", state.toString());
    }

    @Test
    @DisplayName("Preserves all declared transfer state values")
    void preservesTransferStateValues() {
        assertEquals(0, TransferStates.NONE.getValue());
        assertEquals(1, TransferStates.REQUESTED.getValue());
        assertEquals(2, TransferStates.QUEUED.getValue());
        assertEquals(4, TransferStates.INITIALIZING.getValue());
        assertEquals(8, TransferStates.IN_PROGRESS.getValue());
        assertEquals(16, TransferStates.COMPLETED.getValue());
        assertEquals(32, TransferStates.SUCCEEDED.getValue());
        assertEquals(64, TransferStates.CANCELLED.getValue());
        assertEquals(128, TransferStates.TIMED_OUT.getValue());
        assertEquals(256, TransferStates.ERRORED.getValue());
        assertEquals(512, TransferStates.REJECTED.getValue());
        assertEquals(1024, TransferStates.ABORTED.getValue());
        assertEquals(2048, TransferStates.LOCALLY.getValue());
        assertEquals(4096, TransferStates.REMOTELY.getValue());
    }

    @Test
    @DisplayName("Returns canonical instances for declared single states")
    void returnsCanonicalInstancesForDeclaredStates() {
        assertSame(SoulseekClientStates.CONNECTED, SoulseekClientStates.fromValue(2));
        assertSame(SearchStates.QUEUED, SearchStates.fromValue(256));
        assertSame(TransferStates.REJECTED, TransferStates.fromValue(512));
    }

    @Test
    @DisplayName("Uses value equality for combined states")
    void usesValueEqualityForCombinedStates() {
        SearchStates first = SearchStates.COMPLETED.or(SearchStates.ERRORED);
        SearchStates second = SearchStates.fromValue(132);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertNotEquals(first, SearchStates.COMPLETED);
    }

    @Test
    @DisplayName("None is present in every flags value like Enum.HasFlag")
    void noneIsPresentInEveryFlagsValue() {
        assertTrue(SoulseekClientStates.CONNECTED.hasFlag(SoulseekClientStates.NONE));
        assertTrue(SearchStates.COMPLETED.hasFlag(SearchStates.NONE));
        assertTrue(TransferStates.IN_PROGRESS.hasFlag(TransferStates.NONE));
    }
}
