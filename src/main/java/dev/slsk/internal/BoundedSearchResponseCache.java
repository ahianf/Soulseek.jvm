// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Holds search responses we could not deliver, so they can still go out if the
 * requester reaches us later.
 *
 * <p>Answering a search means connecting back to the peer who searched. When
 * that connection cannot be made, a {@code ConnectToPeer} solicitation goes
 * through the server and the response waits here against the solicitation
 * token; if the peer punches through, the listener looks the token up and the
 * answer is written on the connection they opened. Without somewhere to keep it
 * the answer is simply lost, which is the common case for a peer behind NAT —
 * that is to say, most of them.
 *
 * <p>This was an application's job, supplied through an option. It is not one:
 * the token, the lifetime and the punch-through are all protocol facts, the
 * application has no way to know them, and a client without a cache silently
 * answers fewer searches than it thinks it does. Every consumer that wanted to
 * answer searches at all wrote the same class.
 *
 * <p>Entries expire because nothing ever tells us a solicitation failed, and a
 * response nobody collected is worthless within seconds anyway: the searcher has
 * moved on and the token will never be seen again. The bound is there because we
 * answer as many searches as the network sends, and an unreachable peer costs an
 * entry every time.
 */
public final class BoundedSearchResponseCache implements SearchResponseCache {

    /** How long an undelivered response is worth keeping. */
    private static final long TTL_MILLIS = 60_000;

    /** How many to keep at once. */
    private static final int MAXIMUM_ENTRIES = 1_000;

    private record Entry(SearchResponseCacheRecord record, long expiresAtMillis) {}

    private final ConcurrentMap<Integer, Entry> entries = new ConcurrentHashMap<>();
    private final long ttlMillis;
    private final int maximumEntries;

    /** Creates a cache with the default lifetime and bound. */
    public BoundedSearchResponseCache() {
        this(TTL_MILLIS, MAXIMUM_ENTRIES);
    }

    /**
     * Creates a cache.
     *
     * @param ttlMillis how long an undelivered response is worth keeping
     * @param maximumEntries how many to keep at once
     */
    BoundedSearchResponseCache(long ttlMillis, int maximumEntries) {
        this.ttlMillis = ttlMillis;
        this.maximumEntries = maximumEntries;
    }

    @Override
    public void put(int responseToken, SearchResponseCacheRecord response) {
        long now = System.currentTimeMillis();
        entries.values().removeIf(entry -> entry.expiresAtMillis() <= now);
        if (entries.size() >= maximumEntries) {
            List<Map.Entry<Integer, Entry>> oldest = entries.entrySet().stream()
                    .sorted(Comparator.comparingLong(entry -> entry.getValue().expiresAtMillis()))
                    .limit(Math.max(1, entries.size() - maximumEntries + 1))
                    .toList();
            oldest.forEach(entry -> entries.remove(entry.getKey(), entry.getValue()));
        }
        entries.put(responseToken, new Entry(response, now + ttlMillis));
    }

    @Override
    public CacheLookupResult<SearchResponseCacheRecord> lookup(int responseToken) {
        Entry entry = entries.get(responseToken);
        if (entry == null) {
            return CacheLookupResult.notFound();
        }
        if (entry.expiresAtMillis() <= System.currentTimeMillis()) {
            entries.remove(responseToken, entry);
            return CacheLookupResult.notFound();
        }
        return CacheLookupResult.found(entry.record());
    }

    @Override
    public CacheLookupResult<SearchResponseCacheRecord> remove(int responseToken) {
        Entry entry = entries.remove(responseToken);
        return entry == null ? CacheLookupResult.notFound() : CacheLookupResult.found(entry.record());
    }
}
