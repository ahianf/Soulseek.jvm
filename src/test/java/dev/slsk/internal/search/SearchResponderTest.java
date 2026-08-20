// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.Subscription;
import dev.slsk.internal.common.Outcomes;
import dev.slsk.internal.common.TokenFactory;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.diagnostics.DiagnosticMessage;
import dev.slsk.internal.diagnostics.DiagnosticSeverity;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.events.SearchRequestEvent;
import dev.slsk.internal.events.SearchRequestResponseEvent;
import dev.slsk.internal.messaging.messages.ConnectToPeerResponse;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.PeerConnectionManager;
import dev.slsk.internal.network.PeerEndpoint;
import dev.slsk.internal.network.TransferConnectionResult;
import dev.slsk.internal.network.tcp.TransportConnection;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.share.File;
import dev.slsk.search.SearchFile;
import dev.slsk.spi.ShareCatalog;
import dev.slsk.user.Username;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class SearchResponderTest {
    /** What the catalog matches, and what the responder must encode from it. */
    private static final List<SearchFile> MATCHES =
            List.of(new SearchFile("shared\\song.mp3", 42L, dev.slsk.search.FileAttributes.none()));

    /** What the responder is told we upload at, and what a response must advertise. */
    private static final int ADVERTISED_SPEED = 4321;

    private static SearchResponseMessage expectedResponse(int token) {
        return dev.slsk.internal.share.Catalogs.searchResponse("me", token, MATCHES, true, ADVERTISED_SPEED, 0);
    }

    private static final SearchResponseMessage RESPONSE =
            new SearchResponseMessage("local", 7, true, 1, 0, List.of(new File(1, "file", 2, "ext")));

    @Test
    void constructorValidatesPortsAndUsesSuppliedDiagnostic() {
        Fixture nulls = fixture(null);
        assertThrows(
                NullPointerException.class,
                () -> new DefaultSearchResponder(
                        null,
                        () -> nulls.manager,
                        new TokenFactory(77),
                        nulls.client::endpoint,
                        nulls.client::getShareCatalog,
                        () -> "me",
                        () -> ADVERTISED_SPEED));
        Fixture fixture = fixture(null);
        RecordingDiagnostic diagnostic = new RecordingDiagnostic();
        DefaultSearchResponder responder = responder(fixture.client, diagnostic);
        assertSame(diagnostic, responder.getDiagnostic());
    }

    @Test
    void defaultDiagnosticRaisesTypedEventsAndAllowsNoListeners() {
        Fixture fixture = fixture(null);
        DefaultSearchResponder responder = responder(fixture.client, null);
        AtomicReference<DiagnosticMessage> event = new AtomicReference<>();
        Consumer<dev.slsk.internal.diagnostics.DiagnosticMessage> listener = args -> event.set(args);
        Subscription subscription = responder.subscribe(listener);
        responder.getDiagnostic().info("test");
        assertEquals("test", event.get().message());
        subscription.close();
        responder.getDiagnostic().info("unbound");
    }

    @Test
    void tryDiscardHandlesMissingFoundAndThrowingCaches() {
        assertFalse(fixture(null).responder.tryDiscard(1));

        TestCache cache = new TestCache();
        SearchResponseCacheRecord record = new SearchResponseCacheRecord("alice", 2, "q", RESPONSE);
        cache.removed = java.util.Optional.of(record);
        Fixture fixture = fixture(cache);
        AtomicReference<SearchRequestResponseEvent> failed = new AtomicReference<>();
        fixture.responder.<SearchRequestResponseEvent>subscribe(
                SearchResponder.Kind.RESPONSE_DELIVERY_FAILED, args -> failed.set(args));
        assertTrue(fixture.responder.tryDiscard(9));
        assertEquals(9, cache.lastRemovedToken);
        assertEquals("alice", failed.get().username());
        assertSame(RESPONSE, failed.get().searchResponse());
        assertTrue(fixture.diagnostic.debug.stream().anyMatch(text -> text.contains("Discarded cached")));

        cache.throwOnRemove = new RuntimeException("cache");
        assertFalse(fixture.responder.tryDiscard(10));
        assertSame(cache.throwOnRemove, fixture.diagnostic.lastThrowable);
    }

    @Test
    void requestEventAlwaysPrecedesNullResolverResult() {
        Fixture fixture = fixture(null);
        AtomicReference<SearchRequestEvent> request = new AtomicReference<>();
        fixture.responder.<SearchRequestEvent>subscribe(
                SearchResponder.Kind.REQUEST_RECEIVED, args -> request.set(args));

        assertFalse(fixture.responder.tryRespond("alice", 4, "query"));
        assertEquals("alice", request.get().username());
        assertEquals(4, request.get().token());
        assertEquals("query", request.get().query());
    }

    @Test
    void aCatalogThatFailsOrMatchesNothingAnswersNothing() {
        Fixture throwing = catalogFixture(null, null);
        assertFalse(throwing.responder.tryRespond("alice", 1, "q"));
        assertEquals("resolver", throwing.diagnostic.lastThrowable.getMessage());

        Fixture empty = catalogFixture(List.of(), null);
        assertFalse(empty.responder.tryRespond("alice", 1, "q"));
    }

    @Test
    void resolvedResponseConnectsWritesAndRaisesDelivered() {
        Fixture fixture = catalogFixture(MATCHES, null);
        AtomicReference<byte[]> written = new AtomicReference<>();
        fixture.manager.connection = messageConnection(written, CompletableFuture.completedFuture(null));
        AtomicReference<SearchRequestResponseEvent> delivered = new AtomicReference<>();
        fixture.responder.<SearchRequestResponseEvent>subscribe(
                SearchResponder.Kind.RESPONSE_DELIVERED, args -> delivered.set(args));

        assertTrue(fixture.responder.tryRespond("alice", 3, "query"));

        assertEquals("alice", fixture.manager.lastUsername);
        assertEquals(77, fixture.manager.lastSolicitationToken);
        assertArrayEquals(expectedResponse(3).toByteArray(), written.get());
        assertArrayEquals(
                expectedResponse(3).toByteArray(),
                delivered.get().searchResponse().toByteArray());
        assertTrue(fixture.diagnostic.debug.stream().anyMatch(text -> text.startsWith("Resolved")));
        assertTrue(fixture.diagnostic.debug.stream().anyMatch(text -> text.startsWith("Sent response containing")));
    }

    @Test
    void connectFailureCachesResponseAndCacheFailureIsWarning() {
        TestCache cache = new TestCache();
        Fixture fixture = catalogFixture(MATCHES, cache);
        RuntimeException connect = new RuntimeException("connect");
        fixture.manager.connectionFailure = connect;

        assertFalse(fixture.responder.tryRespond("alice", 3, "query"));
        assertEquals(77, cache.lastAddedToken);
        assertEquals("alice", cache.added.username());
        assertArrayEquals(
                expectedResponse(3).toByteArray(), cache.added.searchResponse().toByteArray());

        RuntimeException cacheFailure = new RuntimeException("cache");
        cache.throwOnAdd = cacheFailure;
        assertFalse(fixture.responder.tryRespond("alice", 3, "query"));
        assertSame(cacheFailure, fixture.diagnostic.lastWarningThrowable);
    }

    @Test
    void aResponseEvictedBeforeDeliveryIsReportedAsFailed() {
        TestCache cache = new TestCache();
        Fixture fixture = catalogFixture(MATCHES, cache);
        fixture.manager.connectionFailure = new RuntimeException("connect");
        AtomicReference<SearchRequestResponseEvent> failed = new AtomicReference<>();
        fixture.responder.<SearchRequestResponseEvent>subscribe(
                SearchResponder.Kind.RESPONSE_DELIVERY_FAILED, args -> failed.set(args));

        assertFalse(fixture.responder.tryRespond("alice", 3, "query"));

        // Caching is what binds the listener: a response can only be evicted
        // from a cache it was put into.
        assertNotNull(cache.evictionListener, "the responder never registered for evictions");
        cache.evictionListener.accept(cache.added);

        assertNotNull(failed.get(), "an evicted response raised no failure event");
        assertEquals("alice", failed.get().username());
        assertEquals(3, failed.get().token());
        assertTrue(fixture.diagnostic.debug.stream()
                .anyMatch(text -> text.startsWith("Expired undelivered search response to alice")));
    }

    @Test
    void endpointAndWriteFailuresReturnFalseWithoutCachingWriteFailure() {
        Fixture endpoint = catalogFixture(MATCHES, new TestCache());
        endpoint.client.endpointFailure = new RuntimeException("endpoint");
        assertFalse(endpoint.responder.tryRespond("alice", 3, "query"));
        assertEquals(0, endpoint.client.cache.addCount);

        Fixture write = catalogFixture(MATCHES, new TestCache());
        write.manager.connection = messageConnection(
                new AtomicReference<>(), CompletableFuture.failedFuture(new RuntimeException("write")));
        assertFalse(write.responder.tryRespond("alice", 3, "query"));
        assertEquals(0, write.client.cache.addCount);
    }

    @Test
    void cachedResponseHandlesMissingCacheEntryAndCacheFailure() {
        assertFalse(fixture(null).responder.tryRespond(1));

        TestCache cache = new TestCache();
        Fixture missing = fixture(cache);
        assertFalse(missing.responder.tryRespond(2));

        RuntimeException failure = new RuntimeException("cache");
        cache.throwOnRemove = failure;
        assertFalse(missing.responder.tryRespond(3));
        assertSame(failure, missing.diagnostic.lastThrowable);
    }

    @Test
    void cachedResponseWritesAndRaisesDelivered() {
        TestCache cache = new TestCache();
        cache.removed = java.util.Optional.of(new SearchResponseCacheRecord("alice", 3, "query", RESPONSE));
        Fixture fixture = fixture(cache);
        AtomicReference<byte[]> written = new AtomicReference<>();
        fixture.manager.connection = messageConnection(written, CompletableFuture.completedFuture(null));
        AtomicInteger delivered = new AtomicInteger();
        fixture.responder.<SearchRequestResponseEvent>subscribe(
                SearchResponder.Kind.RESPONSE_DELIVERED, args -> delivered.incrementAndGet());

        assertTrue(fixture.responder.tryRespond(44));
        assertArrayEquals(RESPONSE.toByteArray(), written.get());
        assertEquals(1, delivered.get());
        assertTrue(fixture.diagnostic.debug.stream().anyMatch(text -> text.contains("Sent cached response 44")));
    }

    @Test
    void cachedDeliveryFailureRaisesFailureEvent() {
        TestCache cache = new TestCache();
        cache.removed = java.util.Optional.of(new SearchResponseCacheRecord("alice", 3, "query", RESPONSE));
        Fixture fixture = fixture(cache);
        RuntimeException failure = new RuntimeException("write");
        fixture.manager.connection =
                messageConnection(new AtomicReference<>(), CompletableFuture.failedFuture(failure));
        AtomicReference<SearchRequestResponseEvent> failed = new AtomicReference<>();
        fixture.responder.<SearchRequestResponseEvent>subscribe(
                SearchResponder.Kind.RESPONSE_DELIVERY_FAILED, args -> failed.set(args));

        assertFalse(fixture.responder.tryRespond(44));
        assertSame(RESPONSE, failed.get().searchResponse());
        assertSame(failure, fixture.diagnostic.lastThrowable);
    }

    private static Fixture fixture(TestCache cache) {
        TestCache actualCache = cache;
        SoulseekClientOptions options = options(actualCache);
        TestPeerManager manager = new TestPeerManager();
        TestClient client = new TestClient(options, manager, actualCache);
        RecordingDiagnostic diagnostic = new RecordingDiagnostic();
        return new Fixture(responder(client, diagnostic), client, manager, diagnostic);
    }

    /**
     * A responder answering from a catalog rather than a resolver.
     *
     * @param matches what the catalog returns for any search, or {@code null} to
     *     make the catalog throw
     * @param cache the response cache, or {@code null} for none
     * @return the fixture
     */
    private static Fixture catalogFixture(List<SearchFile> matches, TestCache cache) {
        Fixture fixture = fixture(cache);
        fixture.client.catalog = new ShareCatalog() {
            @Override
            public dev.slsk.share.BrowseResponse browse(Username requester) {
                return dev.slsk.share.BrowseResponse.empty();
            }

            @Override
            public List<dev.slsk.share.Directory> directory(Username requester, String path) {
                return List.of();
            }

            @Override
            public List<SearchFile> search(Username requester, String terms, int limit) {
                if (matches == null) {
                    throw new IllegalStateException("resolver");
                }
                return matches;
            }

            @Override
            public java.util.Optional<dev.slsk.spi.ResolvedFile> resolve(Username requester, String path) {
                return java.util.Optional.empty();
            }

            @Override
            public dev.slsk.share.ShareIndex index() {
                return dev.slsk.share.ShareIndex.empty();
            }
        };
        return fixture;
    }

    private static SoulseekClientOptions options(SearchResponseCache cache) {
        return SoulseekClientOptions.builder()
                .minimumDiagnosticLevel(DiagnosticSeverity.TRACE)
                .searchResponseCache(cache)
                .build();
    }

    private static MessageConnection messageConnection(
            AtomicReference<byte[]> written, CompletableFuture<Void> writeResult) {
        return (MessageConnection) Proxy.newProxyInstance(
                SearchResponderTest.class.getClassLoader(),
                new Class<?>[] {MessageConnection.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("write") && arguments[0] instanceof byte[] bytes) {
                        written.set(bytes);
                        Outcomes.raise(writeResult);
                        return null;
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

    /** The values the responder is built from, kept together for the tests. */
    private static final class TestClient {

        private volatile ShareCatalog catalog = ShareCatalog.empty();

        private ShareCatalog getShareCatalog() {
            return catalog;
        }

        private final SoulseekClientOptions options;
        private final TestPeerManager manager;
        private final TestCache cache;
        private RuntimeException endpointFailure;

        private TestClient(SoulseekClientOptions options, TestPeerManager manager, TestCache cache) {
            this.options = options;
            this.manager = manager;
            this.cache = cache;
        }

        private InetSocketAddress endpoint(String username, CancellationSignal cancellationSignal) {
            if (endpointFailure != null) {
                throw endpointFailure;
            }
            return new InetSocketAddress("127.0.0.1", 1234);
        }
    }

    /** Builds a responder over a test client's values. */
    private static DefaultSearchResponder responder(TestClient client, RecordingDiagnostic diagnostic) {
        return new DefaultSearchResponder(
                () -> client.options,
                () -> client.manager,
                new TokenFactory(77),
                client::endpoint,
                client::getShareCatalog,
                () -> "me",
                () -> ADVERTISED_SPEED,
                diagnostic);
    }

    private static final class TestCache implements SearchResponseCache {
        private java.util.Optional<SearchResponseCacheRecord> removed = java.util.Optional.empty();
        private RuntimeException throwOnRemove;
        private RuntimeException throwOnAdd;
        private int lastRemovedToken;
        private int lastAddedToken;
        private int addCount;
        private SearchResponseCacheRecord added;
        private Consumer<SearchResponseCacheRecord> evictionListener;

        @Override
        public void setEvictionListener(Consumer<SearchResponseCacheRecord> listener) {
            evictionListener = listener;
        }

        @Override
        public void put(int responseToken, SearchResponseCacheRecord response) {
            if (throwOnAdd != null) {
                throw throwOnAdd;
            }
            lastAddedToken = responseToken;
            added = response;
            addCount++;
        }

        @Override
        public java.util.Optional<SearchResponseCacheRecord> lookup(int responseToken) {
            return java.util.Optional.empty();
        }

        @Override
        public java.util.Optional<SearchResponseCacheRecord> remove(int responseToken) {
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
        public void addOrUpdateMessageConnection(String username, TransportConnection incomingConnection) {}

        @Override
        public TransportConnection awaitTransferConnection(
                String username, String filename, int remoteToken, CancellationSignal cancellationSignal) {
            return unsupported();
        }

        @Override
        public MessageConnection getCachedMessageConnection(String username) {
            if (connectionFailure != null) {
                throw connectionFailure;
            }
            return connection;
        }

        @Override
        public MessageConnection getOrAddMessageConnection(ConnectToPeerResponse response) {
            return unsupported();
        }

        @Override
        public MessageConnection getOrAddMessageConnection(
                String username, InetSocketAddress ipEndpoint, CancellationSignal cancellationSignal) {
            return getOrAddMessageConnection(username, ipEndpoint, 0, cancellationSignal);
        }

        @Override
        public MessageConnection getOrAddMessageConnection(
                String username,
                InetSocketAddress ipEndpoint,
                int solicitationToken,
                CancellationSignal cancellationSignal) {
            lastUsername = username;
            lastSolicitationToken = solicitationToken;
            if (connectionFailure != null) {
                throw connectionFailure;
            }
            return connection;
        }

        @Override
        public TransferConnectionResult getTransferConnection(
                String username, int token, TransportConnection incomingConnection) {
            return unsupported();
        }

        @Override
        public TransferConnectionResult getTransferConnection(ConnectToPeerResponse response) {
            return unsupported();
        }

        @Override
        public TransportConnection getTransferConnection(
                String username, InetSocketAddress ipEndpoint, int token, CancellationSignal cancellationSignal) {
            return unsupported();
        }

        @Override
        public void removeAndCloseAll() {}

        @Override
        public boolean tryInvalidateMessageConnectionCache(String username) {
            return false;
        }

        @Override
        public Subscription subscribe(Consumer<? super DiagnosticMessage> listener) {
            return () -> {};
        }

        @Override
        public void close() {}

        private static <T> T unsupported() {
            throw new UnsupportedOperationException();
        }
    }
}
