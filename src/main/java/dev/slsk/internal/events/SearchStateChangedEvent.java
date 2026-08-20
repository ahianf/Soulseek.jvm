// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.search.SearchPhase;
import dev.slsk.internal.search.SearchSnapshot;
import java.util.Objects;

/** Event payload emitted by a search-state change. */
public record SearchStateChangedEvent(SearchPhase previousState, SearchSnapshot search) implements SoulseekClientEvent {

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
