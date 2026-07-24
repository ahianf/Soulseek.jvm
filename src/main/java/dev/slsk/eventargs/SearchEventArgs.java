// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import dev.slsk.Search;

/**
 * Base event arguments for search events.
 */
public abstract class SearchEventArgs extends SoulseekClientEventArgs {
    private final Search search;

    /**
     * Creates search event arguments.
     *
     * @param search the search that raised the event
     */
    protected SearchEventArgs(Search search) {
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
