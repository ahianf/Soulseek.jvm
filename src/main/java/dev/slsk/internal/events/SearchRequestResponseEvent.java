// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.search.SearchResponse;

/** Event payload for the disposition of a search-request response. */
public class SearchRequestResponseEvent extends SearchRequestEvent {
    private final SearchResponse searchResponse;

    /**
     * Creates response-disposition event payload.
     *
     * @param username the requesting username
     * @param token the request token
     * @param query the query text
     * @param searchResponse the resolved response
     */
    public SearchRequestResponseEvent(String username, int token, String query, SearchResponse searchResponse) {
        super(username, token, query);
        this.searchResponse = searchResponse;
    }

    /** Returns the resolved response. */
    public final SearchResponse getSearchResponse() {
        return searchResponse;
    }
}
