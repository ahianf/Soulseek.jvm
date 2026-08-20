// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.common.CacheLookupResult;
import dev.slsk.internal.search.SearchResponseCache;
import dev.slsk.internal.search.SearchResponseCacheRecord;
import dev.slsk.internal.user.UserEndpointCache;
import java.net.InetAddress;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SoulseekClientOptionsPatchTest {
    @Test
    void instantiatesWithGivenData() throws Exception {
        ConnectionOptions server = ConnectionOptions.builder()
                .readBufferSize(1)
                .writeBufferSize(2)
                .writeQueueSize(3)
                .connectTimeout(Duration.ofMillis(4))
                .inactivityTimeout(Duration.ofMillis(5))
                .build();
        ConnectionOptions peer = new ConnectionOptions();
        ConnectionOptions transfer = new ConnectionOptions();
        ConnectionOptions incoming = new ConnectionOptions();
        ConnectionOptions distributed = new ConnectionOptions();
        UserEndpointCache userCache = new TestUserCache();
        SearchResponseCache searchCache = new TestSearchCache();
        InetAddress address = InetAddress.getByName("127.0.0.2");

        SoulseekClientOptionsPatch patch = SoulseekClientOptionsPatch.builder()
                .enableListener(false)
                .listenIpAddress(address)
                .listenPort(1234)
                .enableDistributedNetwork(false)
                .acceptDistributedChildren(false)
                .distributedChildLimit(6)
                .maximumUploadSpeed(7)
                .maximumDownloadSpeed(8)
                .deduplicateSearchRequests(false)
                .autoAcknowledgePrivateMessages(false)
                .autoAcknowledgePrivilegeNotifications(false)
                .acceptPrivateRoomInvitations(true)
                .serverConnectionOptions(server)
                .peerConnectionOptions(peer)
                .transferConnectionOptions(transfer)
                .incomingConnectionOptions(incoming)
                .distributedConnectionOptions(distributed)
                .userEndpointCache(userCache)
                .searchResponseCache(searchCache)
                .build();

        assertEquals(Optional.of(false), patch.enableListener());
        assertEquals(Optional.of(address), patch.listenIpAddress());
        assertEquals(Optional.of(1234), patch.listenPort());
        assertEquals(Optional.of(false), patch.enableDistributedNetwork());
        assertEquals(Optional.of(false), patch.acceptDistributedChildren());
        assertEquals(Optional.of(6), patch.distributedChildLimit());
        assertEquals(Optional.of(7), patch.maximumUploadSpeed());
        assertEquals(Optional.of(8), patch.maximumDownloadSpeed());
        assertEquals(Optional.of(false), patch.deduplicateSearchRequests());
        assertEquals(Optional.of(false), patch.autoAcknowledgePrivateMessages());
        assertEquals(Optional.of(false), patch.autoAcknowledgePrivilegeNotifications());
        assertEquals(Optional.of(true), patch.acceptPrivateRoomInvitations());
        assertNull(patch.serverConnectionOptions().orElseThrow().inactivityTimeout());
        assertSame(peer, patch.peerConnectionOptions().orElseThrow());
        assertSame(transfer, patch.transferConnectionOptions().orElseThrow());
        assertSame(incoming, patch.incomingConnectionOptions().orElseThrow());
        assertSame(distributed, patch.distributedConnectionOptions().orElseThrow());
        assertSame(userCache, patch.userEndpointCache().orElseThrow());
        assertSame(searchCache, patch.searchResponseCache().orElseThrow());
    }

    @Test
    void emptyPatchContainsOnlyEmptyOptionals() {
        SoulseekClientOptionsPatch patch = new SoulseekClientOptionsPatch();

        assertTrue(patch.enableListener().isEmpty());
        assertTrue(patch.listenIpAddress().isEmpty());
        assertTrue(patch.listenPort().isEmpty());
        assertTrue(patch.serverConnectionOptions().isEmpty());
        assertTrue(patch.transferConnectionOptions().isEmpty());
    }

    @Test
    void validatesPortBeforeDistributedChildLimit() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> SoulseekClientOptionsPatch.builder()
                        .listenPort(1023)
                        .distributedChildLimit(-1)
                        .build());

        assertEquals("listenPort must be between 1024 and 65535", exception.getMessage());
    }

    @Test
    void validatesHighPortAndNegativeChildLimit() {
        assertThrows(
                IllegalArgumentException.class,
                () -> SoulseekClientOptionsPatch.builder().listenPort(65_536).build());
        assertThrows(
                IllegalArgumentException.class,
                () -> SoulseekClientOptionsPatch.builder()
                        .distributedChildLimit(-1)
                        .build());
    }

    @Test
    void builderLeavesUnspecifiedFieldsEmpty() throws Exception {
        InetAddress address = InetAddress.getLoopbackAddress();

        assertTrue(SoulseekClientOptionsPatch.builder()
                .enableListener(false)
                .build()
                .listenIpAddress()
                .isEmpty());
        assertTrue(SoulseekClientOptionsPatch.builder()
                .enableListener(false)
                .listenIpAddress(address)
                .build()
                .listenPort()
                .isEmpty());
        assertTrue(SoulseekClientOptionsPatch.builder()
                .enableListener(false)
                .listenIpAddress(address)
                .listenPort(1234)
                .build()
                .enableDistributedNetwork()
                .isEmpty());
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
