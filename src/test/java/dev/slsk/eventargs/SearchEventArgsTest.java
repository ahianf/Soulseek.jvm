// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.Search;
import dev.slsk.SearchQuery;
import dev.slsk.SearchScope;
import dev.slsk.SearchStates;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SearchEventArgsTest {
    @Test
    @DisplayName("SearchStateChangedEventArgs instantiates with valid Search")
    void stateChangedInstantiatesWithValidSearch() {
        Search search =
                new Search(new SearchQuery("foo"), SearchScope.getNetwork(), 42, SearchStates.COMPLETED, 0, 0, 0);
        SearchStateChangedEventArgs args = new SearchStateChangedEventArgs(SearchStates.NONE, search);

        assertSame(search, args.getSearch());
        assertEquals(SearchStates.NONE, args.getPreviousState());
        assertEquals(SearchStates.COMPLETED, args.getSearch().getState());
    }

    @Test
    @DisplayName("SearchRequestEventArgs instantiates with context")
    void requestInstantiatesWithContext() {
        SearchRequestEventArgs args = new SearchRequestEventArgs("alice", -1, "foo");

        assertEquals("alice", args.getUsername());
        assertEquals(-1, args.getToken());
        assertEquals("foo", args.getQuery());
    }

    @Test
    @DisplayName("SearchRequestEventArgs preserves nullable references")
    void requestPreservesNullableReferences() {
        SearchRequestEventArgs args = new SearchRequestEventArgs(null, 0, null);

        assertNull(args.getUsername());
        assertNull(args.getQuery());
    }

    @Test
    @DisplayName("Search event base preserves a null search")
    void searchEventBasePreservesNullSearch() {
        SearchStateChangedEventArgs args = new SearchStateChangedEventArgs(SearchStates.NONE, null);

        assertNull(args.getSearch());
    }

    @Test
    @DisplayName("Rejects null previous state because C# flags are non-nullable")
    void rejectsNullPreviousState() {
        assertThrows(NullPointerException.class, () -> new SearchStateChangedEventArgs(null, null));
    }
}
