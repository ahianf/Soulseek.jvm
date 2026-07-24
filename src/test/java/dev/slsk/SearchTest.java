// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

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
        SearchStates state = SearchStates.COMPLETED.or(SearchStates.TIMED_OUT);

        Search search = new Search(query, scope, 42, state, 3, 4, 5);

        assertEquals("foo bar", search.getQuery().getSearchText());
        assertEquals(SearchScopeType.NETWORK, search.getScope().getType());
        assertEquals(42, search.getToken());
        assertEquals(state, search.getState());
        assertEquals(3, search.getResponseCount());
        assertEquals(4, search.getFileCount());
        assertEquals(5, search.getLockedFileCount());
    }

    @Test
    @DisplayName("Preserves nullable query and scope references")
    void preservesNullableQueryAndScopeReferences() {
        Search search = new Search(null, null, 0, SearchStates.NONE, 0, 0, 0);

        assertNull(search.getQuery());
        assertNull(search.getScope());
    }

    @Test
    @DisplayName("Rejects null state because the C# flag enum is non-nullable")
    void rejectsNullState() {
        assertThrows(NullPointerException.class, () -> new Search(null, null, 0, null, 0, 0, 0));
    }
}
