// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Remembers where a peer was, so the next thing we send them does not cost a
 * round trip to the server.
 *
 * <p>Answering a search means connecting back to the peer who searched, and
 * connecting means knowing their address, which only the server can tell us.
 * Peers search repeatedly: over a nineteen-hour sample, 58,721 answered
 * searches came from 7,046 distinct peers, so seven of every eight address
 * lookups asked the server something it had already been asked. Every one of
 * them also blocked the answer until the reply came back, and 256 of them
 * blocked it for the full ten-second timeout before giving up.
 *
 * <p>This was an application's job, supplied through an option, and it was
 * left null. The consequence is not a slow client but a rude one: a lookup per
 * answered search is a rate no client should put on a single central server,
 * and nothing about it is the embedder's decision to make.
 *
 * <p>Entries expire because a peer's address is only true until they
 * reconnect. A stale one costs a direct connection attempt that fails —
 * nothing more, because the indirect attempt races alongside it and is routed
 * by the server on username, not on anything cached here. That is the whole
 * risk, and it is why the lifetime can afford to be generous: at ten minutes
 * the same sample would have avoided 45.6% of its lookups holding at most 462
 * entries, against 27.2% at five minutes and 70.8% at thirty.
 */
public final class BoundedUserEndpointCache implements UserEndpointCache {

    /** How long a peer's address is worth believing. */
    private static final long TTL_MILLIS = 600_000;

    /** How many to keep at once. */
    private static final int MAXIMUM_ENTRIES = 2_000;

    private record Entry(InetSocketAddress endpoint, long expiresAtMillis) {}

    private final ConcurrentMap<String, Entry> entries = new ConcurrentHashMap<>();
    private final long ttlMillis;
    private final int maximumEntries;

    /** Creates a cache with the default lifetime and bound. */
    public BoundedUserEndpointCache() {
        this(TTL_MILLIS, MAXIMUM_ENTRIES);
    }

    /**
     * Creates a cache.
     *
     * @param ttlMillis how long a peer's address is worth believing
     * @param maximumEntries how many to keep at once
     */
    BoundedUserEndpointCache(long ttlMillis, int maximumEntries) {
        this.ttlMillis = ttlMillis;
        this.maximumEntries = maximumEntries;
    }

    @Override
    public void put(String username, InetSocketAddress endpoint) {
        long now = System.currentTimeMillis();
        entries.values().removeIf(entry -> entry.expiresAtMillis() <= now);
        if (entries.size() >= maximumEntries) {
            List<Map.Entry<String, Entry>> oldest = entries.entrySet().stream()
                    .sorted(Comparator.comparingLong(entry -> entry.getValue().expiresAtMillis()))
                    .limit(Math.max(1, entries.size() - maximumEntries + 1))
                    .toList();
            oldest.forEach(entry -> entries.remove(entry.getKey(), entry.getValue()));
        }
        entries.put(username, new Entry(endpoint, now + ttlMillis));
    }

    @Override
    public CacheLookupResult<InetSocketAddress> lookup(String username) {
        Entry entry = entries.get(username);
        if (entry == null) {
            return CacheLookupResult.notFound();
        }
        if (entry.expiresAtMillis() <= System.currentTimeMillis()) {
            entries.remove(username, entry);
            return CacheLookupResult.notFound();
        }
        return CacheLookupResult.found(entry.endpoint());
    }
}
