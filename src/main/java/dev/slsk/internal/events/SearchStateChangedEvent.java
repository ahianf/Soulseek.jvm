// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.search.Search;
import dev.slsk.internal.search.SearchState;
import java.util.Objects;

/**
 * Event arguments raised by a search-state change.
 */
public class SearchStateChangedEvent extends SearchEvent {
    private final SearchState previousState;

    /**
     * Creates search-state event payload.
     *
     * <p>The C# constructor is assembly-internal. Java has no equivalent
     * visibility spanning the client and event-argument packages, so the
     * direct port exposes this constructor.</p>
     *
     * @param previousState the state before the change
     * @param search the search after the change
     */
    public SearchStateChangedEvent(SearchState previousState, Search search) {
        super(search);
        this.previousState = Objects.requireNonNull(previousState, "previousState");
    }

    /**
     * Returns the previous search state.
     *
     * @return the previous state
     */
    public final SearchState getPreviousState() {
        return previousState;
    }
}
