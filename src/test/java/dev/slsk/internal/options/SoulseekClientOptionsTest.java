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

import dev.slsk.internal.CacheLookupResult;
import dev.slsk.internal.SearchResponseCache;
import dev.slsk.internal.SearchResponseCacheRecord;
import dev.slsk.internal.UserEndpointCache;
import dev.slsk.internal.diagnostics.DiagnosticLevel;
import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class SoulseekClientOptionsTest {
    @Test
    void instantiatesWithGivenData() throws Exception {
        ConnectionOptions server = new ConnectionOptions(1, 2, 3, 4, 5);
        ConnectionOptions peer = new ConnectionOptions();
        ConnectionOptions transfer = new ConnectionOptions();
        ConnectionOptions incoming = new ConnectionOptions();
        ConnectionOptions distributed = new ConnectionOptions();
        UserEndpointCache userCache = new TestUserCache();
        SearchResponseCache searchCache = new TestSearchCache();
        SearchResponseResolver searchResolver = (user, token, query) -> CompletableFuture.completedFuture(null);
        BrowseResponseResolver browseResolver = (user, endpoint) -> CompletableFuture.completedFuture(null);
        DirectoryContentsResolver directoryResolver =
                (user, endpoint, token, directory) -> CompletableFuture.completedFuture(null);
        UserInfoResolver infoResolver = (user, endpoint) -> CompletableFuture.completedFuture(null);
        EnqueueDownloadCallback enqueue = (user, endpoint, filename) -> CompletableFuture.completedFuture(null);
        PlaceInQueueResolver place = (user, endpoint, filename) -> CompletableFuture.completedFuture(0);
        InetAddress address = InetAddress.getByName("127.0.0.2");

        SoulseekClientOptions options = new SoulseekClientOptions(
                false,
                address,
                1234,
                false,
                false,
                6,
                7,
                8,
                9,
                10,
                11,
                false,
                12,
                false,
                false,
                true,
                DiagnosticLevel.TRACE,
                13,
                server,
                peer,
                transfer,
                incoming,
                distributed,
                userCache,
                searchResolver,
                searchCache,
                browseResolver,
                directoryResolver,
                infoResolver,
                enqueue,
                place,
                true);

        assertFalse(options.isEnableListener());
        assertSame(address, options.getListenIpAddress());
        assertEquals(1234, options.getListenPort());
        assertFalse(options.isEnableDistributedNetwork());
        assertFalse(options.isAcceptDistributedChildren());
        assertEquals(6, options.getDistributedChildLimit());
        assertEquals(7, options.getMaximumConcurrentSearches());
        assertEquals(8, options.getMaximumConcurrentUploads());
        assertEquals(9, options.getMaximumUploadSpeed());
        assertEquals(10, options.getMaximumConcurrentDownloads());
        assertEquals(11, options.getMaximumDownloadSpeed());
        assertFalse(options.isDeduplicateSearchRequests());
        assertEquals(12, options.getMessageTimeout());
        assertFalse(options.isAutoAcknowledgePrivateMessages());
        assertFalse(options.isAutoAcknowledgePrivilegeNotifications());
        assertTrue(options.isAcceptPrivateRoomInvitations());
        assertEquals(DiagnosticLevel.TRACE, options.getMinimumDiagnosticLevel());
        assertEquals(13, options.getStartingToken());
        assertEquals(-1, options.getServerConnectionOptions().getInactivityTimeout());
        assertSame(peer, options.getPeerConnectionOptions());
        assertSame(transfer, options.getTransferConnectionOptions());
        assertSame(incoming, options.getIncomingConnectionOptions());
        assertSame(distributed, options.getDistributedConnectionOptions());
        assertSame(userCache, options.getUserEndpointCache());
        assertSame(searchResolver, options.getSearchResponseResolver());
        assertSame(searchCache, options.getSearchResponseCache());
        assertSame(browseResolver, options.getBrowseResponseResolver());
        assertSame(directoryResolver, options.getDirectoryContentsResolver());
        assertSame(infoResolver, options.getUserInfoResolver());
        assertSame(enqueue, options.getEnqueueDownload());
        assertSame(place, options.getPlaceInQueueResolver());
        assertTrue(options.isRaiseEventsAsynchronously());
        assertEquals(1, options.getMaximumConcurrentUploadsPerUser());
    }

    @Test
    void sourceDefaultsAndDefaultResolvers() {
        SoulseekClientOptions options = new SoulseekClientOptions();

        assertTrue(options.isEnableListener());
        assertEquals("0.0.0.0", options.getListenIpAddress().getHostAddress());
        assertEquals(50_000, options.getListenPort());
        assertTrue(options.isEnableDistributedNetwork());
        assertTrue(options.isAcceptDistributedChildren());
        assertEquals(25, options.getDistributedChildLimit());
        assertEquals(2, options.getMaximumConcurrentSearches());
        assertEquals(10, options.getMaximumConcurrentUploads());
        assertEquals(Integer.MAX_VALUE, options.getMaximumUploadSpeed());
        assertEquals(Integer.MAX_VALUE, options.getMaximumConcurrentDownloads());
        assertEquals(Integer.MAX_VALUE, options.getMaximumDownloadSpeed());
        assertTrue(options.isDeduplicateSearchRequests());
        assertEquals(5_000, options.getMessageTimeout());
        assertTrue(options.isAutoAcknowledgePrivateMessages());
        assertTrue(options.isAutoAcknowledgePrivilegeNotifications());
        assertFalse(options.isAcceptPrivateRoomInvitations());
        assertEquals(DiagnosticLevel.INFO, options.getMinimumDiagnosticLevel());
        assertNotNull(options.getServerConnectionOptions());
        assertEquals(-1, options.getServerConnectionOptions().getInactivityTimeout());
        assertNotNull(options.getPeerConnectionOptions());
        assertNotNull(options.getTransferConnectionOptions());
        assertNotNull(options.getIncomingConnectionOptions());
        assertNotNull(options.getDistributedConnectionOptions());
        assertNull(options.getSearchResponseResolver());
        assertNull(options.getDirectoryContentsResolver());
        assertEquals(
                0, options.getBrowseResponseResolver().resolve("", null).join().getDirectoryCount());
        assertEquals("", options.getUserInfoResolver().resolve("", null).join().getDescription());
        assertNull(options.getEnqueueDownload().enqueue("", null, "").join());
        assertNull(options.getPlaceInQueueResolver().resolve("", null, "").join());
    }

    @Test
    void configuresListenerPortAndMessageTimeout() {
        SoulseekClientOptions options = new SoulseekClientOptions(true, null, 50_001, 15_000);

        assertTrue(options.isEnableListener());
        assertEquals(50_001, options.getListenPort());
        assertEquals(15_000, options.getMessageTimeout());
        assertThrows(IllegalArgumentException.class, () -> new SoulseekClientOptions(true, null, 50_001, 0));

        SoulseekClientOptions traced = new SoulseekClientOptions(true, null, 50_001, 15_000, DiagnosticLevel.DEBUG);
        assertEquals(DiagnosticLevel.DEBUG, traced.getMinimumDiagnosticLevel());
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
        ConnectionOptions peer = new ConnectionOptions(7);
        SoulseekClientOptions original = complete(1234, 2, 44, 42, 24);
        SoulseekClientOptionsPatch patch = new SoulseekClientOptionsPatch(
                false,
                InetAddress.getLoopbackAddress(),
                2345,
                false,
                false,
                3,
                4,
                5,
                false,
                false,
                false,
                true,
                null,
                peer,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);

        SoulseekClientOptions copy = original.with(patch);

        assertFalse(copy.isEnableListener());
        assertEquals(2345, copy.getListenPort());
        assertFalse(copy.isEnableDistributedNetwork());
        assertFalse(copy.isAcceptDistributedChildren());
        assertEquals(3, copy.getDistributedChildLimit());
        assertEquals(4, copy.getMaximumUploadSpeed());
        assertEquals(5, copy.getMaximumDownloadSpeed());
        assertFalse(copy.isDeduplicateSearchRequests());
        assertFalse(copy.isAutoAcknowledgePrivateMessages());
        assertFalse(copy.isAutoAcknowledgePrivilegeNotifications());
        assertTrue(copy.isAcceptPrivateRoomInvitations());
        assertSame(peer, copy.getPeerConnectionOptions());
        assertEquals(42, copy.getMaximumConcurrentUploads());
        assertEquals(24, copy.getMaximumConcurrentDownloads());
        assertEquals(DiagnosticLevel.NONE, copy.getMinimumDiagnosticLevel());

        // The source's internal With overload omits these two values.
        assertEquals(2, copy.getMaximumConcurrentSearches());
        assertFalse(copy.isRaiseEventsAsynchronously());
    }

    @Test
    void withRejectsNullAndEmptyPatchRetainsPatchableValues() {
        SoulseekClientOptions original = complete(1234, 7, 8, 9, 10);

        assertThrows(NullPointerException.class, () -> original.with(null));
        SoulseekClientOptions copy = original.with(new SoulseekClientOptionsPatch());
        assertEquals(1234, copy.getListenPort());
        assertEquals(9, copy.getMaximumConcurrentUploads());
        assertEquals(10, copy.getMaximumConcurrentDownloads());
    }

    @Test
    void prefixOverloadsPreserveTrailingDefaults() {
        assertEquals(
                "0.0.0.0", new SoulseekClientOptions(false).getListenIpAddress().getHostAddress());
        assertEquals(50_000, new SoulseekClientOptions(false, InetAddress.getLoopbackAddress()).getListenPort());
        assertTrue(
                new SoulseekClientOptions(false, InetAddress.getLoopbackAddress(), 1234).isEnableDistributedNetwork());
    }

    private static SoulseekClientOptions complete(
            int listenPort, int childLimit, int searches, int uploads, int downloads) {
        return new SoulseekClientOptions(
                true,
                null,
                listenPort,
                true,
                true,
                childLimit,
                searches,
                uploads,
                Integer.MAX_VALUE,
                downloads,
                Integer.MAX_VALUE,
                true,
                5_000,
                true,
                true,
                false,
                DiagnosticLevel.NONE,
                0,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                true);
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
