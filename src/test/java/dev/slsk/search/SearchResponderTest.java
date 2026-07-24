// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.search;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CacheLookupResult;
import dev.slsk.CancellationToken;
import dev.slsk.File;
import dev.slsk.ISearchResponseCache;
import dev.slsk.SearchResponse;
import dev.slsk.SearchResponseCacheRecord;
import dev.slsk.diagnostics.DiagnosticEventArgs;
import dev.slsk.diagnostics.DiagnosticEventListener;
import dev.slsk.diagnostics.DiagnosticLevel;
import dev.slsk.diagnostics.DiagnosticSink;
import dev.slsk.eventargs.SearchRequestEventArgs;
import dev.slsk.eventargs.SearchRequestResponseEventArgs;
import dev.slsk.messaging.messages.ConnectToPeerResponse;
import dev.slsk.network.MessageConnection;
import dev.slsk.network.PeerConnectionManager;
import dev.slsk.network.PeerEndpoint;
import dev.slsk.network.TransferConnectionResult;
import dev.slsk.network.tcp.Connection;
import dev.slsk.options.ConnectionOptions;
import dev.slsk.options.SearchResponseResolver;
import dev.slsk.options.SoulseekClientOptions;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class SearchResponderTest {
    private static final SearchResponse RESPONSE =
            new SearchResponse("local", 7, true, 1, 0, List.of(new File(1, "file", 2, "ext")));

    @Test
    void constructorValidatesClientAndUsesSuppliedDiagnostic() {
        assertThrows(NullPointerException.class, () -> new DefaultSearchResponder(null));
        Fixture fixture = fixture(null, null);
        RecordingDiagnostic diagnostic = new RecordingDiagnostic();
        DefaultSearchResponder responder = new DefaultSearchResponder(fixture.client, diagnostic);
        assertSame(diagnostic, responder.getDiagnostic());
    }

    @Test
    void defaultDiagnosticRaisesTypedEventsAndAllowsNoListeners() {
        Fixture fixture = fixture(null, null);
        DefaultSearchResponder responder = new DefaultSearchResponder(fixture.client);
        AtomicReference<DiagnosticEventArgs> event = new AtomicReference<>();
        DiagnosticEventListener listener = (sender, args) -> event.set(args);
        responder.addDiagnosticGeneratedListener(listener);
        responder.getDiagnostic().info("test");
        assertEquals("test", event.get().getMessage());
        responder.removeDiagnosticGeneratedListener(listener);
        responder.getDiagnostic().info("unbound");
    }

    @Test
    void tryDiscardHandlesMissingFoundAndThrowingCaches() {
        assertFalse(fixture(null, null).responder.tryDiscard(1));

        TestCache cache = new TestCache();
        SearchResponseCacheRecord record = new SearchResponseCacheRecord("alice", 2, "q", RESPONSE);
        cache.removed = CacheLookupResult.found(record);
        Fixture fixture = fixture(null, cache);
        AtomicReference<SearchRequestResponseEventArgs> failed = new AtomicReference<>();
        fixture.responder.addResponseDeliveryFailedListener((sender, args) -> failed.set(args));
        assertTrue(fixture.responder.tryDiscard(9));
        assertEquals(9, cache.lastRemovedToken);
        assertEquals("alice", failed.get().getUsername());
        assertSame(RESPONSE, failed.get().getSearchResponse());
        assertTrue(fixture.diagnostic.debug.stream().anyMatch(text -> text.contains("Discarded cached")));

        cache.throwOnRemove = new RuntimeException("cache");
        assertFalse(fixture.responder.tryDiscard(10));
        assertSame(cache.throwOnRemove, fixture.diagnostic.lastThrowable);
    }

    @Test
    void requestEventAlwaysPrecedesNullResolverResult() {
        Fixture fixture = fixture(null, null);
        AtomicReference<SearchRequestEventArgs> request = new AtomicReference<>();
        fixture.responder.addRequestReceivedListener((sender, args) -> request.set(args));

        assertFalse(fixture.responder.tryRespondAsync("alice", 4, "query").join());
        assertEquals("alice", request.get().getUsername());
        assertEquals(4, request.get().getToken());
        assertEquals("query", request.get().getQuery());
    }

    @Test
    void resolverFailuresNullAndEmptyResponsesReturnFalse() {
        RuntimeException failure = new RuntimeException("resolver");
        Fixture throwing = fixture((user, token, query) -> CompletableFuture.failedFuture(failure), null);
        assertFalse(throwing.responder.tryRespondAsync("alice", 1, "q").join());
        assertSame(failure, throwing.diagnostic.lastThrowable);

        Fixture nullResponse = fixture((user, token, query) -> CompletableFuture.completedFuture(null), null);
        assertFalse(nullResponse.responder.tryRespondAsync("alice", 1, "q").join());

        Fixture empty = fixture(
                (user, token, query) ->
                        CompletableFuture.completedFuture(new SearchResponse("x", 1, false, 0, 0, null)),
                null);
        assertFalse(empty.responder.tryRespondAsync("alice", 1, "q").join());
    }

    @Test
    void resolvedResponseConnectsWritesAndRaisesDelivered() {
        Fixture fixture = fixture((user, token, query) -> CompletableFuture.completedFuture(RESPONSE), null);
        AtomicReference<byte[]> written = new AtomicReference<>();
        fixture.manager.connection = messageConnection(written, CompletableFuture.completedFuture(null));
        AtomicReference<SearchRequestResponseEventArgs> delivered = new AtomicReference<>();
        fixture.responder.addResponseDeliveredListener((sender, args) -> delivered.set(args));

        assertTrue(fixture.responder.tryRespondAsync("alice", 3, "query").join());

        assertEquals("alice", fixture.manager.lastUsername);
        assertEquals(77, fixture.manager.lastSolicitationToken);
        assertArrayEquals(RESPONSE.toByteArray(), written.get());
        assertSame(RESPONSE, delivered.get().getSearchResponse());
        assertTrue(fixture.diagnostic.debug.stream().anyMatch(text -> text.startsWith("Resolved")));
        assertTrue(fixture.diagnostic.debug.stream().anyMatch(text -> text.startsWith("Sent response containing")));
    }

    @Test
    void connectFailureCachesResponseAndCacheFailureIsWarning() {
        TestCache cache = new TestCache();
        Fixture fixture = fixture((user, token, query) -> CompletableFuture.completedFuture(RESPONSE), cache);
        RuntimeException connect = new RuntimeException("connect");
        fixture.manager.connectionFailure = connect;

        assertFalse(fixture.responder.tryRespondAsync("alice", 3, "query").join());
        assertEquals(77, cache.lastAddedToken);
        assertEquals("alice", cache.added.username());
        assertSame(RESPONSE, cache.added.searchResponse());

        RuntimeException cacheFailure = new RuntimeException("cache");
        cache.throwOnAdd = cacheFailure;
        assertFalse(fixture.responder.tryRespondAsync("alice", 3, "query").join());
        assertSame(cacheFailure, fixture.diagnostic.lastWarningThrowable);
    }

    @Test
    void endpointAndWriteFailuresReturnFalseWithoutCachingWriteFailure() {
        Fixture endpoint =
                fixture((user, token, query) -> CompletableFuture.completedFuture(RESPONSE), new TestCache());
        endpoint.client.endpointFailure = new RuntimeException("endpoint");
        assertFalse(endpoint.responder.tryRespondAsync("alice", 3, "query").join());
        assertEquals(0, endpoint.client.cache.addCount);

        Fixture write = fixture((user, token, query) -> CompletableFuture.completedFuture(RESPONSE), new TestCache());
        write.manager.connection = messageConnection(
                new AtomicReference<>(), CompletableFuture.failedFuture(new RuntimeException("write")));
        assertFalse(write.responder.tryRespondAsync("alice", 3, "query").join());
        assertEquals(0, write.client.cache.addCount);
    }

    @Test
    void cachedResponseHandlesMissingCacheEntryAndCacheFailure() {
        assertFalse(fixture(null, null).responder.tryRespondAsync(1).join());

        TestCache cache = new TestCache();
        Fixture missing = fixture(null, cache);
        assertFalse(missing.responder.tryRespondAsync(2).join());

        RuntimeException failure = new RuntimeException("cache");
        cache.throwOnRemove = failure;
        assertFalse(missing.responder.tryRespondAsync(3).join());
        assertSame(failure, missing.diagnostic.lastThrowable);
    }

    @Test
    void cachedResponseWritesAndRaisesDelivered() {
        TestCache cache = new TestCache();
        cache.removed = CacheLookupResult.found(new SearchResponseCacheRecord("alice", 3, "query", RESPONSE));
        Fixture fixture = fixture(null, cache);
        AtomicReference<byte[]> written = new AtomicReference<>();
        fixture.manager.connection = messageConnection(written, CompletableFuture.completedFuture(null));
        AtomicInteger delivered = new AtomicInteger();
        fixture.responder.addResponseDeliveredListener((sender, args) -> delivered.incrementAndGet());

        assertTrue(fixture.responder.tryRespondAsync(44).join());
        assertArrayEquals(RESPONSE.toByteArray(), written.get());
        assertEquals(1, delivered.get());
        assertTrue(fixture.diagnostic.debug.stream().anyMatch(text -> text.contains("Sent cached response 44")));
    }

    @Test
    void cachedDeliveryFailureRaisesFailureEvent() {
        TestCache cache = new TestCache();
        cache.removed = CacheLookupResult.found(new SearchResponseCacheRecord("alice", 3, "query", RESPONSE));
        Fixture fixture = fixture(null, cache);
        RuntimeException failure = new RuntimeException("write");
        fixture.manager.connection =
                messageConnection(new AtomicReference<>(), CompletableFuture.failedFuture(failure));
        AtomicReference<SearchRequestResponseEventArgs> failed = new AtomicReference<>();
        fixture.responder.addResponseDeliveryFailedListener((sender, args) -> failed.set(args));

        assertFalse(fixture.responder.tryRespondAsync(44).join());
        assertSame(RESPONSE, failed.get().getSearchResponse());
        assertSame(failure, fixture.diagnostic.lastThrowable);
    }

    private static Fixture fixture(SearchResponseResolver resolver, TestCache cache) {
        TestCache actualCache = cache;
        SoulseekClientOptions options = options(resolver, actualCache);
        TestPeerManager manager = new TestPeerManager();
        TestClient client = new TestClient(options, manager, actualCache);
        RecordingDiagnostic diagnostic = new RecordingDiagnostic();
        return new Fixture(new DefaultSearchResponder(client, diagnostic), client, manager, diagnostic);
    }

    private static SoulseekClientOptions options(SearchResponseResolver resolver, ISearchResponseCache cache) {
        return new SoulseekClientOptions(
                true,
                null,
                50_000,
                true,
                true,
                25,
                2,
                10,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                true,
                5_000,
                true,
                true,
                false,
                DiagnosticLevel.TRACE,
                0,
                new ConnectionOptions(),
                new ConnectionOptions(),
                new ConnectionOptions(),
                new ConnectionOptions(),
                new ConnectionOptions(),
                null,
                resolver,
                cache,
                null,
                null,
                null,
                null,
                null,
                false);
    }

    private static MessageConnection messageConnection(
            AtomicReference<byte[]> written, CompletableFuture<Void> writeResult) {
        return (MessageConnection) Proxy.newProxyInstance(
                SearchResponderTest.class.getClassLoader(),
                new Class<?>[] {MessageConnection.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("writeAsync") && arguments[0] instanceof byte[] bytes) {
                        written.set(bytes);
                        return writeResult;
                    }
                    if (method.getName().equals("toString")) {
                        return "messageConnection";
                    }
                    return defaultValue(method.getReturnType());
                });
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        return null;
    }

    private record Fixture(
            DefaultSearchResponder responder,
            TestClient client,
            TestPeerManager manager,
            RecordingDiagnostic diagnostic) {}

    private static final class TestClient implements SearchResponderClient {
        private final SoulseekClientOptions options;
        private final TestPeerManager manager;
        private final TestCache cache;
        private RuntimeException endpointFailure;

        private TestClient(SoulseekClientOptions options, TestPeerManager manager, TestCache cache) {
            this.options = options;
            this.manager = manager;
            this.cache = cache;
        }

        @Override
        public SoulseekClientOptions getOptions() {
            return options;
        }

        @Override
        public PeerConnectionManager getPeerConnectionManager() {
            return manager;
        }

        @Override
        public int getNextToken() {
            return 77;
        }

        @Override
        public CompletableFuture<InetSocketAddress> getUserEndPointAsync(
                String username, CancellationToken cancellationToken) {
            if (endpointFailure != null) {
                return CompletableFuture.failedFuture(endpointFailure);
            }
            return CompletableFuture.completedFuture(new InetSocketAddress("127.0.0.1", 1234));
        }
    }

    private static final class TestCache implements ISearchResponseCache {
        private CacheLookupResult<SearchResponseCacheRecord> removed = CacheLookupResult.notFound();
        private RuntimeException throwOnRemove;
        private RuntimeException throwOnAdd;
        private int lastRemovedToken;
        private int lastAddedToken;
        private int addCount;
        private SearchResponseCacheRecord added;

        @Override
        public void addOrUpdate(int responseToken, SearchResponseCacheRecord response) {
            if (throwOnAdd != null) {
                throw throwOnAdd;
            }
            lastAddedToken = responseToken;
            added = response;
            addCount++;
        }

        @Override
        public CacheLookupResult<SearchResponseCacheRecord> tryGet(int responseToken) {
            return CacheLookupResult.notFound();
        }

        @Override
        public CacheLookupResult<SearchResponseCacheRecord> tryRemove(int responseToken) {
            if (throwOnRemove != null) {
                throw throwOnRemove;
            }
            lastRemovedToken = responseToken;
            return removed;
        }
    }

    private static final class RecordingDiagnostic implements DiagnosticSink {
        private final java.util.ArrayList<String> debug = new java.util.ArrayList<>();
        private Throwable lastThrowable;
        private Throwable lastWarningThrowable;

        @Override
        public void trace(String message) {}

        @Override
        public void trace(String message, Throwable exception) {}

        @Override
        public void debug(String message) {
            debug.add(message);
        }

        @Override
        public void debug(String message, Throwable exception) {
            debug.add(message);
            lastThrowable = exception;
        }

        @Override
        public void info(String message) {}

        @Override
        public void warning(String message) {}

        @Override
        public void warning(String message, Throwable exception) {
            lastThrowable = exception;
            lastWarningThrowable = exception;
        }
    }

    private static final class TestPeerManager implements PeerConnectionManager {
        private MessageConnection connection;
        private RuntimeException connectionFailure;
        private String lastUsername;
        private int lastSolicitationToken;

        @Override
        public List<PeerEndpoint> getMessageConnections() {
            return List.of();
        }

        @Override
        public Map<Integer, String> getPendingSolicitations() {
            return Map.of();
        }

        @Override
        public CompletableFuture<Void> addOrUpdateMessageConnectionAsync(
                String username, Connection incomingConnection) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Connection> awaitTransferConnectionAsync(
                String username, String filename, int remoteToken, CancellationToken cancellationToken) {
            return unsupported();
        }

        @Override
        public CompletableFuture<MessageConnection> getCachedMessageConnectionAsync(String username) {
            if (connectionFailure != null) {
                return CompletableFuture.failedFuture(connectionFailure);
            }
            return CompletableFuture.completedFuture(connection);
        }

        @Override
        public CompletableFuture<MessageConnection> getOrAddMessageConnectionAsync(ConnectToPeerResponse response) {
            return unsupported();
        }

        @Override
        public CompletableFuture<MessageConnection> getOrAddMessageConnectionAsync(
                String username, InetSocketAddress ipEndPoint, CancellationToken cancellationToken) {
            return getOrAddMessageConnectionAsync(username, ipEndPoint, 0, cancellationToken);
        }

        @Override
        public CompletableFuture<MessageConnection> getOrAddMessageConnectionAsync(
                String username,
                InetSocketAddress ipEndPoint,
                int solicitationToken,
                CancellationToken cancellationToken) {
            lastUsername = username;
            lastSolicitationToken = solicitationToken;
            if (connectionFailure != null) {
                return CompletableFuture.failedFuture(connectionFailure);
            }
            return CompletableFuture.completedFuture(connection);
        }

        @Override
        public CompletableFuture<TransferConnectionResult> getTransferConnectionAsync(
                String username, int token, Connection incomingConnection) {
            return unsupported();
        }

        @Override
        public CompletableFuture<TransferConnectionResult> getTransferConnectionAsync(ConnectToPeerResponse response) {
            return unsupported();
        }

        @Override
        public CompletableFuture<Connection> getTransferConnectionAsync(
                String username, InetSocketAddress ipEndPoint, int token, CancellationToken cancellationToken) {
            return unsupported();
        }

        @Override
        public void removeAndDisposeAll() {}

        @Override
        public boolean tryInvalidateMessageConnectionCache(String username) {
            return false;
        }

        @Override
        public void addDiagnosticGeneratedListener(DiagnosticEventListener listener) {}

        @Override
        public void removeDiagnosticGeneratedListener(DiagnosticEventListener listener) {}

        @Override
        public void close() {}

        private static <T> CompletableFuture<T> unsupported() {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }
    }
}
