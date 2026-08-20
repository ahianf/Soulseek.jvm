// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SearchSnapshotTest {
    @Test
    @DisplayName("Instantiates with expected data")
    void instantiatesWithExpectedData() {
        ParsedSearchQuery query = new ParsedSearchQuery("foo bar");
        SearchTarget scope = SearchTarget.getNetwork();
        SearchPhase state = SearchPhase.COMPLETED;

        SearchSnapshot search = new SearchSnapshot(query, scope, 42, state, SearchTermination.TIMED_OUT, 3, 4, 5);

        assertEquals("foo bar", search.query().searchText());
        assertEquals(SearchScopeType.NETWORK, search.scope().type());
        assertEquals(42, search.token());
        assertEquals(state, search.state());
        assertEquals(SearchTermination.TIMED_OUT, search.termination());
        assertEquals(3, search.responseCount());
        assertEquals(4, search.fileCount());
        assertEquals(5, search.lockedFileCount());
    }

    @Test
    @DisplayName("Preserves nullable query and scope references")
    void preservesNullableQueryAndScopeReferences() {
        SearchSnapshot search = new SearchSnapshot(null, null, 0, SearchPhase.NONE, null, 0, 0, 0);

        assertNull(search.query());
        assertNull(search.scope());
    }

    @Test
    @DisplayName("Rejects null state and inconsistent termination data")
    void rejectsNullState() {
        assertThrows(NullPointerException.class, () -> new SearchSnapshot(null, null, 0, null, null, 0, 0, 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SearchSnapshot(null, null, 0, SearchPhase.COMPLETED, null, 0, 0, 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SearchSnapshot(null, null, 0, SearchPhase.IN_PROGRESS, SearchTermination.TIMED_OUT, 0, 0, 0));
    }
}
