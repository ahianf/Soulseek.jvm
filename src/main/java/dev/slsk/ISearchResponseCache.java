// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

/** A cache for undelivered search responses. */
public interface ISearchResponseCache {
    /**
     * Adds or updates a response and its context.
     *
     * @param responseToken the delivery-response token
     * @param response the response and routing context
     */
    void addOrUpdate(int responseToken, SearchResponseCacheRecord response);

    /**
     * Attempts to fetch a cached response.
     *
     * @param responseToken the delivery-response token
     * @return the lookup result
     */
    CacheLookupResult<SearchResponseCacheRecord> tryGet(int responseToken);

    /**
     * Attempts to remove a cached response.
     *
     * @param responseToken the delivery-response token
     * @return the removal result
     */
    CacheLookupResult<SearchResponseCacheRecord> tryRemove(int responseToken);
}
