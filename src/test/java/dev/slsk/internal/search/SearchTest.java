// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SearchTest {
    @Test
    @DisplayName("Instantiates with expected data")
    void instantiatesWithExpectedData() {
        SearchQuery query = new SearchQuery("foo bar");
        SearchScope scope = SearchScope.getNetwork();
        SearchState state = SearchState.COMPLETED.or(SearchState.TIMED_OUT);

        Search search = new Search(query, scope, 42, state, 3, 4, 5);

        assertEquals("foo bar", search.query().searchText());
        assertEquals(SearchScopeType.NETWORK, search.scope().type());
        assertEquals(42, search.token());
        assertEquals(state, search.state());
        assertEquals(3, search.responseCount());
        assertEquals(4, search.fileCount());
        assertEquals(5, search.lockedFileCount());
    }

    @Test
    @DisplayName("Preserves nullable query and scope references")
    void preservesNullableQueryAndScopeReferences() {
        Search search = new Search(null, null, 0, SearchState.NONE, 0, 0, 0);

        assertNull(search.query());
        assertNull(search.scope());
    }

    @Test
    @DisplayName("Rejects null state because the C# flag enum is non-nullable")
    void rejectsNullState() {
        assertThrows(NullPointerException.class, () -> new Search(null, null, 0, null, 0, 0, 0));
    }
}
