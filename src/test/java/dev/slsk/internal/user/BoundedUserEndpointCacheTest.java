// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.user;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;

class BoundedUserEndpointCacheTest {

    private static InetSocketAddress endpoint(int port) {
        return new InetSocketAddress("127.0.0.1", port);
    }

    @Test
    void returnsWhatItWasGiven() {
        BoundedUserEndpointCache cache = new BoundedUserEndpointCache();

        cache.put("alice", endpoint(2234));

        java.util.Optional<InetSocketAddress> result = cache.lookup("alice");
        assertTrue(result.isPresent());
        assertEquals(endpoint(2234), result.get());
    }

    @Test
    void missesOnAUsernameItNeverSaw() {
        assertFalse(new BoundedUserEndpointCache().lookup("nobody").isPresent());
    }

    @Test
    void theNewestAddressWins() {
        BoundedUserEndpointCache cache = new BoundedUserEndpointCache();

        cache.put("alice", endpoint(2234));
        cache.put("alice", endpoint(50300));

        assertEquals(endpoint(50300), cache.lookup("alice").get());
    }

    @Test
    void anExpiredEntryIsAMiss() {
        BoundedUserEndpointCache cache = new BoundedUserEndpointCache(-1, 10);

        cache.put("alice", endpoint(2234));

        assertFalse(cache.lookup("alice").isPresent());
    }

    @Test
    void expiryOnLookupDropsTheEntry() {
        BoundedUserEndpointCache cache = new BoundedUserEndpointCache(-1, 10);
        cache.put("alice", endpoint(2234));

        cache.lookup("alice");

        // A second lookup can only miss for the same reason if the first left
        // the entry behind; this pins the removal, not just the miss.
        assertFalse(cache.lookup("alice").isPresent());
    }

    @Test
    void staysWithinItsBound() {
        BoundedUserEndpointCache cache = new BoundedUserEndpointCache(60_000, 3);

        for (int index = 0; index < 25; index++) {
            cache.put("peer" + index, endpoint(2234 + index));
        }

        int live = 0;
        for (int index = 0; index < 25; index++) {
            if (cache.lookup("peer" + index).isPresent()) {
                live++;
            }
        }
        assertTrue(live <= 3, "bound of 3 exceeded: " + live + " entries live");
    }

    @Test
    void keepsTheMostRecentlyWrittenWhenItEvicts() {
        BoundedUserEndpointCache cache = new BoundedUserEndpointCache(60_000, 2);

        cache.put("first", endpoint(1));
        cache.put("second", endpoint(2));
        cache.put("third", endpoint(3));

        assertTrue(cache.lookup("third").isPresent(), "the entry just written was evicted");
    }
}
