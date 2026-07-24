// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import dev.slsk.Search;
import dev.slsk.SearchResponse;

/** Event arguments raised when a search response is received. */
public class SearchResponseReceivedEventArgs extends SearchEventArgs {
    private final SearchResponse response;

    /**
     * Creates search-response event arguments.
     *
     * @param response the received response
     * @param search the receiving search snapshot
     */
    public SearchResponseReceivedEventArgs(SearchResponse response, Search search) {
        super(search);
        this.response = response;
    }

    /** Returns the received response. */
    public final SearchResponse getResponse() {
        return response;
    }
}
