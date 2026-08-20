// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.internal.search.ParsedSearchQuery;
import dev.slsk.internal.search.SearchPhase;
import dev.slsk.internal.search.SearchResponseMessage;
import dev.slsk.internal.search.SearchStateSnapshot;
import dev.slsk.internal.search.SearchTarget;
import dev.slsk.internal.search.SearchTermination;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SearchEventTest {
    @Test
    @DisplayName("SearchStateChangedEvent instantiates with valid SearchStateSnapshot")
    void stateChangedInstantiatesWithValidSearch() {
        SearchStateSnapshot search = new SearchStateSnapshot(
                new ParsedSearchQuery("foo"),
                SearchTarget.getNetwork(),
                42,
                SearchPhase.COMPLETED,
                SearchTermination.TIMED_OUT,
                0,
                0,
                0);
        SearchStateChangedEvent args = new SearchStateChangedEvent(SearchPhase.NONE, search);

        assertSame(search, args.search());
        assertEquals(SearchPhase.NONE, args.previousState());
        assertEquals(SearchPhase.COMPLETED, args.search().state());
    }

    @Test
    @DisplayName("SearchRequestEvent instantiates with context")
    void requestInstantiatesWithContext() {
        SearchRequestEvent args = new SearchRequestEvent("alice", -1, "foo");

        assertEquals("alice", args.username());
        assertEquals(-1, args.token());
        assertEquals("foo", args.query());
    }

    @Test
    @DisplayName("SearchRequestEvent preserves nullable references")
    void requestPreservesNullableReferences() {
        SearchRequestEvent args = new SearchRequestEvent(null, 0, null);

        assertNull(args.username());
        assertNull(args.query());
    }

    @Test
    @DisplayName("SearchStateSnapshot event base preserves a null search")
    void searchEventBasePreservesNullSearch() {
        SearchStateChangedEvent args = new SearchStateChangedEvent(SearchPhase.NONE, null);

        assertNull(args.search());
    }

    @Test
    @DisplayName("Rejects a null previous phase")
    void rejectsNullPreviousState() {
        assertThrows(NullPointerException.class, () -> new SearchStateChangedEvent(null, null));
    }

    @Test
    void responseReceivedInstantiatesWithSearchAndResponse() {
        SearchStateSnapshot search = new SearchStateSnapshot(null, null, 42, SearchPhase.IN_PROGRESS, null, 1, 2, 3);
        SearchResponseMessage response = new SearchResponseMessage("alice", 42, true, 1, 2, null);

        SearchResponseReceivedEvent args = new SearchResponseReceivedEvent(response, search);

        assertSame(search, args.search());
        assertSame(response, args.response());
    }

    @Test
    void requestResponseInstantiatesWithContextAndNullableResponse() {
        SearchRequestResponseEvent args = new SearchRequestResponseEvent("alice", -1, "query", null);

        assertEquals("alice", args.username());
        assertEquals(-1, args.token());
        assertEquals("query", args.query());
        assertNull(args.searchResponse());
    }
}
