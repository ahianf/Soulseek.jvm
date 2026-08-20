// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.search.Search;
import dev.slsk.internal.search.SearchState;
import java.util.Objects;

/** Event payload emitted by a search-state change. */
public record SearchStateChangedEvent(SearchState previousState, Search search) implements SoulseekClientEvent {

    /**
     * Creates search-state event payload.
     *
     * @param previousState the state before the change
     * @param search the search after the change
     */
    public SearchStateChangedEvent {
        Objects.requireNonNull(previousState, "previousState");
    }
}
