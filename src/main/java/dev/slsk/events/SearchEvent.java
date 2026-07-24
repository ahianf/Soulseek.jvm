// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import dev.slsk.Search;

/**
 * Base event payload for search events.
 */
public abstract class SearchEvent extends SoulseekClientEvent {
    private final Search search;

    /**
     * Creates search event payload.
     *
     * @param search the search that raised the event
     */
    protected SearchEvent(Search search) {
        this.search = search;
    }

    /**
     * Returns the search that raised the event.
     *
     * @return the search
     */
    public final Search getSearch() {
        return search;
    }
}
