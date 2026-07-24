// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import dev.slsk.SearchQuery;
import dev.slsk.SearchResponse;
import java.util.concurrent.CompletableFuture;

/** Resolves a response to an incoming search request. */
@FunctionalInterface
public interface SearchResponseResolver {
    /**
     * Resolves a search response.
     *
     * @param username the requesting username
     * @param token the search token
     * @param query the parsed query
     * @return the asynchronous response
     */
    CompletableFuture<SearchResponse> resolve(String username, int token, SearchQuery query);
}
