// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.diagnostics.DiagnosticLevel;
import dev.slsk.internal.search.SearchResponseCache;
import dev.slsk.internal.search.SearchResponseCacheRecord;
import dev.slsk.internal.user.UserEndpointCache;
import java.net.InetAddress;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class SoulseekClientOptionsTest {
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

        SoulseekClientOptions options = SoulseekClientOptions.builder()
                .enableListener(false)
                .listenIpAddress(address)
                .listenPort(1234)
                .enableDistributedNetwork(false)
                .acceptDistributedChildren(false)
                .distributedChildLimit(6)
                .maximumConcurrentSearches(7)
                .maximumConcurrentUploads(8)
                .maximumUploadSpeed(9)
                .maximumConcurrentDownloads(10)
                .maximumDownloadSpeed(11)
                .deduplicateSearchRequests(false)
                .messageTimeout(Duration.ofMillis(12))
                .autoAcknowledgePrivateMessages(false)
                .autoAcknowledgePrivilegeNotifications(false)
                .acceptPrivateRoomInvitations(true)
                .minimumDiagnosticLevel(DiagnosticLevel.TRACE)
                .startingToken(13)
                .serverConnectionOptions(server)
                .peerConnectionOptions(peer)
                .transferConnectionOptions(transfer)
                .incomingConnectionOptions(incoming)
                .distributedConnectionOptions(distributed)
                .userEndpointCache(userCache)
                .searchResponseCache(searchCache)
                .build();

        assertFalse(options.enableListener());
        assertSame(address, options.listenIpAddress());
        assertEquals(1234, options.listenPort());
        assertFalse(options.enableDistributedNetwork());
        assertFalse(options.acceptDistributedChildren());
        assertEquals(6, options.distributedChildLimit());
        assertEquals(7, options.maximumConcurrentSearches());
        assertEquals(8, options.maximumConcurrentUploads());
        assertEquals(9, options.maximumUploadSpeed());
        assertEquals(10, options.maximumConcurrentDownloads());
        assertEquals(11, options.maximumDownloadSpeed());
        assertFalse(options.deduplicateSearchRequests());
        assertEquals(Duration.ofMillis(12), options.messageTimeout());
        assertFalse(options.autoAcknowledgePrivateMessages());
        assertFalse(options.autoAcknowledgePrivilegeNotifications());
        assertTrue(options.acceptPrivateRoomInvitations());
        assertEquals(DiagnosticLevel.TRACE, options.minimumDiagnosticLevel());
        assertEquals(13, options.startingToken());
        assertNull(options.serverConnectionOptions().inactivityTimeout());
        assertSame(peer, options.peerConnectionOptions());
        assertSame(transfer, options.transferConnectionOptions());
        assertSame(incoming, options.incomingConnectionOptions());
        assertSame(distributed, options.distributedConnectionOptions());
        assertSame(userCache, options.userEndpointCache());
        assertSame(searchCache, options.searchResponseCache());
        assertEquals(1, options.maximumConcurrentUploadsPerUser());
    }

    @Test
    void sourceDefaultsAndDefaultResolvers() {
        SoulseekClientOptions options = new SoulseekClientOptions();

        assertTrue(options.enableListener());
        assertEquals("0.0.0.0", options.listenIpAddress().getHostAddress());
        assertEquals(30_000, options.listenPort());
        assertTrue(options.enableDistributedNetwork());
        assertTrue(options.acceptDistributedChildren());
        assertEquals(25, options.distributedChildLimit());
        assertEquals(2, options.maximumConcurrentSearches());
        assertEquals(10, options.maximumConcurrentUploads());
        assertEquals(Integer.MAX_VALUE, options.maximumUploadSpeed());
        assertEquals(Integer.MAX_VALUE, options.maximumConcurrentDownloads());
        assertEquals(Integer.MAX_VALUE, options.maximumDownloadSpeed());
        assertTrue(options.deduplicateSearchRequests());
        assertEquals(Duration.ofSeconds(5), options.messageTimeout());
        assertTrue(options.autoAcknowledgePrivateMessages());
        assertTrue(options.autoAcknowledgePrivilegeNotifications());
        assertFalse(options.acceptPrivateRoomInvitations());
        assertEquals(DiagnosticLevel.INFO, options.minimumDiagnosticLevel());
        assertNotNull(options.serverConnectionOptions());
        assertNull(options.serverConnectionOptions().inactivityTimeout());
        assertNotNull(options.peerConnectionOptions());
        assertNotNull(options.transferConnectionOptions());
        assertNotNull(options.incomingConnectionOptions());
        assertNotNull(options.distributedConnectionOptions());
    }

    @Test
    void configuresListenerPortAndMessageTimeout() {
        SoulseekClientOptions options = SoulseekClientOptions.builder()
                .listenPort(50_001)
                .messageTimeout(Duration.ofSeconds(15))
                .build();

        assertTrue(options.enableListener());
        assertEquals(50_001, options.listenPort());
        assertEquals(Duration.ofSeconds(15), options.messageTimeout());
        assertThrows(
                IllegalArgumentException.class,
                () -> SoulseekClientOptions.builder()
                        .listenPort(50_001)
                        .messageTimeout(Duration.ZERO)
                        .build());

        SoulseekClientOptions traced = SoulseekClientOptions.builder()
                .listenPort(50_001)
                .messageTimeout(Duration.ofSeconds(15))
                .minimumDiagnosticLevel(DiagnosticLevel.DEBUG)
                .build();
        assertEquals(DiagnosticLevel.DEBUG, traced.minimumDiagnosticLevel());
    }

    @Test
    void validatesInSourceOrder() {
        IllegalArgumentException port = assertThrows(IllegalArgumentException.class, () -> complete(1023, -1, 0, 0, 0));
        assertTrue(port.getMessage().startsWith("listenPort"));

        IllegalArgumentException child =
                assertThrows(IllegalArgumentException.class, () -> complete(1024, -1, 0, 0, 0));
        assertTrue(child.getMessage().startsWith("distributedChildLimit"));

        IllegalArgumentException searches =
                assertThrows(IllegalArgumentException.class, () -> complete(1024, 0, 0, 0, 0));
        assertTrue(searches.getMessage().startsWith("maximumConcurrentSearches"));

        IllegalArgumentException uploads =
                assertThrows(IllegalArgumentException.class, () -> complete(1024, 0, 1, 0, 0));
        assertTrue(uploads.getMessage().startsWith("maximumConcurrentUploads"));

        IllegalArgumentException downloads =
                assertThrows(IllegalArgumentException.class, () -> complete(1024, 0, 1, 1, 0));
        assertTrue(downloads.getMessage().startsWith("maximumConcurrentDownloads"));
    }

    @Test
    void withAppliesPatchAndPreservesSourceCloneBehavior() {
        ConnectionOptions peer = ConnectionOptions.builder().readBufferSize(7).build();
        SoulseekClientOptions original = complete(1234, 2, 44, 42, 24);
        SoulseekClientOptionsPatch patch = SoulseekClientOptionsPatch.builder()
                .enableListener(false)
                .listenIpAddress(InetAddress.getLoopbackAddress())
                .listenPort(2345)
                .enableDistributedNetwork(false)
                .acceptDistributedChildren(false)
                .distributedChildLimit(3)
                .maximumUploadSpeed(4)
                .maximumDownloadSpeed(5)
                .deduplicateSearchRequests(false)
                .autoAcknowledgePrivateMessages(false)
                .autoAcknowledgePrivilegeNotifications(false)
                .acceptPrivateRoomInvitations(true)
                .peerConnectionOptions(peer)
                .build();

        SoulseekClientOptions copy = original.with(patch);

        assertFalse(copy.enableListener());
        assertEquals(2345, copy.listenPort());
        assertFalse(copy.enableDistributedNetwork());
        assertFalse(copy.acceptDistributedChildren());
        assertEquals(3, copy.distributedChildLimit());
        assertEquals(4, copy.maximumUploadSpeed());
        assertEquals(5, copy.maximumDownloadSpeed());
        assertFalse(copy.deduplicateSearchRequests());
        assertFalse(copy.autoAcknowledgePrivateMessages());
        assertFalse(copy.autoAcknowledgePrivilegeNotifications());
        assertTrue(copy.acceptPrivateRoomInvitations());
        assertSame(peer, copy.peerConnectionOptions());
        assertEquals(42, copy.maximumConcurrentUploads());
        assertEquals(24, copy.maximumConcurrentDownloads());
        assertEquals(DiagnosticLevel.NONE, copy.minimumDiagnosticLevel());

        // Not patchable, so a customized limit must survive an unrelated patch.
        assertEquals(44, copy.maximumConcurrentSearches());
    }

    @Test
    void withRejectsNullAndEmptyPatchRetainsPatchableValues() {
        SoulseekClientOptions original = complete(1234, 7, 8, 9, 10);

        assertThrows(NullPointerException.class, () -> original.with(null));
        SoulseekClientOptions copy = original.with(new SoulseekClientOptionsPatch());
        assertEquals(1234, copy.listenPort());
        assertEquals(8, copy.maximumConcurrentSearches());
        assertEquals(9, copy.maximumConcurrentUploads());
        assertEquals(10, copy.maximumConcurrentDownloads());
    }

    @Test
    void builderPreservesUnspecifiedDefaults() {
        assertEquals(
                "0.0.0.0",
                SoulseekClientOptions.builder()
                        .enableListener(false)
                        .build()
                        .listenIpAddress()
                        .getHostAddress());
        assertEquals(
                30_000,
                SoulseekClientOptions.builder()
                        .enableListener(false)
                        .listenIpAddress(InetAddress.getLoopbackAddress())
                        .build()
                        .listenPort());
        assertTrue(SoulseekClientOptions.builder()
                .enableListener(false)
                .listenIpAddress(InetAddress.getLoopbackAddress())
                .listenPort(1234)
                .build()
                .enableDistributedNetwork());
    }

    private static SoulseekClientOptions complete(
            int listenPort, int childLimit, int searches, int uploads, int downloads) {
        return SoulseekClientOptions.builder()
                .listenPort(listenPort)
                .distributedChildLimit(childLimit)
                .maximumConcurrentSearches(searches)
                .maximumConcurrentUploads(uploads)
                .maximumConcurrentDownloads(downloads)
                .minimumDiagnosticLevel(DiagnosticLevel.NONE)
                .build();
    }

    private static final class TestUserCache implements UserEndpointCache {
        @Override
        public java.util.Optional<java.net.InetSocketAddress> lookup(String username) {
            return java.util.Optional.empty();
        }

        @Override
        public void put(String username, java.net.InetSocketAddress endpoint) {}
    }

    private static final class TestSearchCache implements SearchResponseCache {
        @Override
        public void put(int responseToken, SearchResponseCacheRecord response) {}

        @Override
        public java.util.Optional<SearchResponseCacheRecord> lookup(int responseToken) {
            return java.util.Optional.empty();
        }

        @Override
        public java.util.Optional<SearchResponseCacheRecord> remove(int responseToken) {
            return java.util.Optional.empty();
        }
    }
}
