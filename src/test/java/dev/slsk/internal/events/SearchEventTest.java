// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.internal.search.Search;
import dev.slsk.internal.search.SearchQuery;
import dev.slsk.internal.search.SearchResponse;
import dev.slsk.internal.search.SearchScope;
import dev.slsk.internal.search.SearchState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SearchEventTest {
    @Test
    @DisplayName("SearchStateChangedEvent instantiates with valid Search")
    void stateChangedInstantiatesWithValidSearch() {
        Search search =
                new Search(new SearchQuery("foo"), SearchScope.getNetwork(), 42, SearchState.COMPLETED, 0, 0, 0);
        SearchStateChangedEvent args = new SearchStateChangedEvent(SearchState.NONE, search);

        assertSame(search, args.getSearch());
        assertEquals(SearchState.NONE, args.getPreviousState());
        assertEquals(SearchState.COMPLETED, args.getSearch().state());
    }

    @Test
    @DisplayName("SearchRequestEvent instantiates with context")
    void requestInstantiatesWithContext() {
        SearchRequestEvent args = new SearchRequestEvent("alice", -1, "foo");

        assertEquals("alice", args.getUsername());
        assertEquals(-1, args.getToken());
        assertEquals("foo", args.getQuery());
    }

    @Test
    @DisplayName("SearchRequestEvent preserves nullable references")
    void requestPreservesNullableReferences() {
        SearchRequestEvent args = new SearchRequestEvent(null, 0, null);

        assertNull(args.getUsername());
        assertNull(args.getQuery());
    }

    @Test
    @DisplayName("Search event base preserves a null search")
    void searchEventBasePreservesNullSearch() {
        SearchStateChangedEvent args = new SearchStateChangedEvent(SearchState.NONE, null);

        assertNull(args.getSearch());
    }

    @Test
    @DisplayName("Rejects null previous state because C# flags are non-nullable")
    void rejectsNullPreviousState() {
        assertThrows(NullPointerException.class, () -> new SearchStateChangedEvent(null, null));
    }

    @Test
    void responseReceivedInstantiatesWithSearchAndResponse() {
        Search search = new Search(null, null, 42, SearchState.IN_PROGRESS, 1, 2, 3);
        SearchResponse response = new SearchResponse("alice", 42, true, 1, 2, null);

        SearchResponseReceivedEvent args = new SearchResponseReceivedEvent(response, search);

        assertSame(search, args.getSearch());
        assertSame(response, args.getResponse());
    }

    @Test
    void requestResponseInstantiatesWithContextAndNullableResponse() {
        SearchRequestResponseEvent args = new SearchRequestResponseEvent("alice", -1, "query", null);

        assertEquals("alice", args.getUsername());
        assertEquals(-1, args.getToken());
        assertEquals("query", args.getQuery());
        assertNull(args.getSearchResponse());
    }
}
