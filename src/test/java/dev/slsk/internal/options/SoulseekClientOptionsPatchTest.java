// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.internal.CacheLookupResult;
import dev.slsk.internal.SearchResponseCache;
import dev.slsk.internal.SearchResponseCacheRecord;
import dev.slsk.internal.UserEndpointCache;
import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class SoulseekClientOptionsPatchTest {
    @Test
    void instantiatesWithGivenData() throws Exception {
        ConnectionOptions server = new ConnectionOptions(1, 2, 3, 4, 5);
        ConnectionOptions peer = new ConnectionOptions();
        ConnectionOptions transfer = new ConnectionOptions();
        ConnectionOptions incoming = new ConnectionOptions();
        ConnectionOptions distributed = new ConnectionOptions();
        UserEndpointCache userCache = new TestUserCache();
        SearchResponseCache searchCache = new TestSearchCache();
        EnqueueDownloadCallback enqueue = (user, endpoint, filename) -> CompletableFuture.completedFuture(null);
        PlaceInQueueResolver place = (user, endpoint, filename) -> CompletableFuture.completedFuture(0);
        InetAddress address = InetAddress.getByName("127.0.0.2");

        SoulseekClientOptionsPatch patch = new SoulseekClientOptionsPatch(
                false,
                address,
                1234,
                false,
                false,
                6,
                7,
                8,
                false,
                false,
                false,
                true,
                server,
                peer,
                transfer,
                incoming,
                distributed,
                userCache,
                searchCache,
                enqueue,
                place);

        assertEquals(false, patch.getEnableListener());
        assertSame(address, patch.getListenIpAddress());
        assertEquals(1234, patch.getListenPort());
        assertEquals(false, patch.getEnableDistributedNetwork());
        assertEquals(false, patch.getAcceptDistributedChildren());
        assertEquals(6, patch.getDistributedChildLimit());
        assertEquals(7, patch.getMaximumUploadSpeed());
        assertEquals(8, patch.getMaximumDownloadSpeed());
        assertEquals(false, patch.getDeduplicateSearchRequests());
        assertEquals(false, patch.getAutoAcknowledgePrivateMessages());
        assertEquals(false, patch.getAutoAcknowledgePrivilegeNotifications());
        assertEquals(true, patch.getAcceptPrivateRoomInvitations());
        assertEquals(-1, patch.getServerConnectionOptions().getInactivityTimeout());
        assertSame(peer, patch.getPeerConnectionOptions());
        assertSame(transfer, patch.getTransferConnectionOptions());
        assertSame(incoming, patch.getIncomingConnectionOptions());
        assertSame(distributed, patch.getDistributedConnectionOptions());
        assertSame(userCache, patch.getUserEndpointCache());
        assertSame(searchCache, patch.getSearchResponseCache());
        assertSame(enqueue, patch.getEnqueueDownload());
        assertSame(place, patch.getPlaceInQueueResolver());
    }

    @Test
    void emptyPatchContainsOnlyNulls() {
        SoulseekClientOptionsPatch patch = new SoulseekClientOptionsPatch();

        assertNull(patch.getEnableListener());
        assertNull(patch.getListenIpAddress());
        assertNull(patch.getListenPort());
        assertNull(patch.getServerConnectionOptions());
        assertNull(patch.getTransferConnectionOptions());
        assertNull(patch.getPlaceInQueueResolver());
    }

    @Test
    void validatesPortBeforeDistributedChildLimit() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new SoulseekClientOptionsPatch(
                        null, null, 1023, null, null, -1, null, null, null, null, null, null, null, null, null, null,
                        null, null, null, null, null));

        assertEquals("listenPort must be between 1024 and 65535", exception.getMessage());
    }

    @Test
    void validatesHighPortAndNegativeChildLimit() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new SoulseekClientOptionsPatch(
                        null, null, 65_536, null, null, null, null, null, null, null, null, null, null, null, null,
                        null, null, null, null, null, null));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SoulseekClientOptionsPatch(
                        null, null, null, null, null, -1, null, null, null, null, null, null, null, null, null, null,
                        null, null, null, null, null));
    }

    @Test
    void prefixOverloadsPreserveTrailingNulls() throws Exception {
        InetAddress address = InetAddress.getLoopbackAddress();

        assertNull(new SoulseekClientOptionsPatch(false).getListenIpAddress());
        assertNull(new SoulseekClientOptionsPatch(false, address).getListenPort());
        assertNull(new SoulseekClientOptionsPatch(false, address, 1234).getEnableDistributedNetwork());
    }

    private static final class TestUserCache implements UserEndpointCache {
        @Override
        public CacheLookupResult<java.net.InetSocketAddress> lookup(String username) {
            return CacheLookupResult.notFound();
        }

        @Override
        public void put(String username, java.net.InetSocketAddress endpoint) {}
    }

    private static final class TestSearchCache implements SearchResponseCache {
        @Override
        public void put(int responseToken, SearchResponseCacheRecord response) {}

        @Override
        public CacheLookupResult<SearchResponseCacheRecord> lookup(int responseToken) {
            return CacheLookupResult.notFound();
        }

        @Override
        public CacheLookupResult<SearchResponseCacheRecord> remove(int responseToken) {
            return CacheLookupResult.notFound();
        }
    }
}
