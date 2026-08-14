// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.CacheLookupResult;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BoundedSearchResponseCacheTest {

    private static SearchResponseCacheRecord record(String username) {
        return new SearchResponseCacheRecord(username, 1, "query", null);
    }

    private static List<String> evictionsOf(BoundedSearchResponseCache cache) {
        List<String> evicted = new ArrayList<>();
        cache.setEvictionListener(record -> evicted.add(record.username()));
        return evicted;
    }

    @Test
    void returnsWhatItWasGiven() {
        BoundedSearchResponseCache cache = new BoundedSearchResponseCache();

        cache.put(1, record("alice"));

        CacheLookupResult<SearchResponseCacheRecord> result = cache.lookup(1);
        assertTrue(result.found());
        assertEquals("alice", result.value().username());
    }

    @Test
    void removeTakesTheRecordOut() {
        BoundedSearchResponseCache cache = new BoundedSearchResponseCache();
        cache.put(1, record("alice"));

        assertEquals("alice", cache.remove(1).value().username());
        assertFalse(cache.lookup(1).found());
    }

    @Test
    void reportsARecordThatExpiredBeforeItWasCollected() {
        BoundedSearchResponseCache cache = new BoundedSearchResponseCache(-1, 10);
        List<String> evicted = evictionsOf(cache);

        cache.put(1, record("alice"));
        assertFalse(cache.lookup(1).found());

        assertEquals(List.of("alice"), evicted);
    }

    @Test
    void reportsRecordsSweptOnTheWayIn() {
        BoundedSearchResponseCache cache = new BoundedSearchResponseCache(-1, 10);
        cache.put(1, record("alice"));
        List<String> evicted = evictionsOf(cache);

        cache.put(2, record("bob"));

        assertEquals(List.of("alice"), evicted);
    }

    @Test
    void reportsARecordPushedOutByTheBound() {
        BoundedSearchResponseCache cache = new BoundedSearchResponseCache(60_000, 2);
        cache.put(1, record("alice"));
        cache.put(2, record("bob"));
        List<String> evicted = evictionsOf(cache);

        cache.put(3, record("carol"));

        assertEquals(1, evicted.size(), "expected exactly one eviction, got " + evicted);
        assertTrue(cache.lookup(3).found(), "the record just written was evicted");
    }

    @Test
    void staysSilentWhenTheCallerTakesTheRecordItself() {
        BoundedSearchResponseCache cache = new BoundedSearchResponseCache();
        cache.put(1, record("alice"));
        List<String> evicted = evictionsOf(cache);

        // A remove is delivery or an explicit discard; both report their own
        // outcome and must not also be counted as an eviction.
        cache.remove(1);

        assertEquals(List.of(), evicted);
    }

    @Test
    void aListenerThatThrowsDoesNotBreakTheCache() {
        BoundedSearchResponseCache cache = new BoundedSearchResponseCache(-1, 10);
        cache.setEvictionListener(record -> {
            throw new IllegalStateException("listener");
        });

        cache.put(1, record("alice"));
        cache.put(2, record("bob"));

        assertFalse(cache.lookup(1).found());
    }
}
