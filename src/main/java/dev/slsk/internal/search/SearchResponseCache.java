// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import dev.slsk.internal.common.CacheLookupResult;
import java.util.function.Consumer;

/** A cache for undelivered search responses. */
public interface SearchResponseCache {
    /**
     * Registers the listener notified when a response leaves the cache without
     * having been delivered.
     *
     * <p>Only unsolicited departures qualify — a lifetime that ran out, or a
     * bound that pushed the oldest entry out. A {@link #remove} is the caller
     * taking the response to do something with it and reports its own outcome.
     *
     * <p>Implementations that never drop an entry on their own need not
     * override this.
     *
     * @param listener the listener, or {@code null} to stop notifying
     */
    default void setEvictionListener(Consumer<SearchResponseCacheRecord> listener) {}

    /**
     * Adds or updates a response and its context.
     *
     * @param responseToken the delivery-response token
     * @param response the response and routing context
     */
    void put(int responseToken, SearchResponseCacheRecord response);

    /**
     * Attempts to fetch a cached response.
     *
     * @param responseToken the delivery-response token
     * @return the lookup result
     */
    CacheLookupResult<SearchResponseCacheRecord> lookup(int responseToken);

    /**
     * Attempts to remove a cached response.
     *
     * @param responseToken the delivery-response token
     * @return the removal result
     */
    CacheLookupResult<SearchResponseCacheRecord> remove(int responseToken);
}
