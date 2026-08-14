// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.search.Search;
import dev.slsk.internal.search.SearchResponse;

/** Event arguments raised when a search response is received. */
public class SearchResponseReceivedEvent extends SearchEvent {
    private final SearchResponse response;

    /**
     * Creates search-response event payload.
     *
     * @param response the received response
     * @param search the receiving search snapshot
     */
    public SearchResponseReceivedEvent(SearchResponse response, Search search) {
        super(search);
        this.response = response;
    }

    /** Returns the received response. */
    public final SearchResponse getResponse() {
        return response;
    }
}
