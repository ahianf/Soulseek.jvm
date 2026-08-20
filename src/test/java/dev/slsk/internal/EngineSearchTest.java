// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.exceptions.DuplicateTokenException;
import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.common.Outcomes;
import dev.slsk.internal.concurrent.CancellationController;
import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.messaging.messages.RoomSearchRequest;
import dev.slsk.internal.messaging.messages.UserSearchRequest;
import dev.slsk.internal.messaging.messages.WishlistSearchRequest;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.options.SearchOptions;
import dev.slsk.internal.options.SearchResponseReceived;
import dev.slsk.internal.options.SearchStateChange;
import dev.slsk.internal.search.Search;
import dev.slsk.internal.search.SearchInternal;
import dev.slsk.internal.search.SearchQuery;
import dev.slsk.internal.search.SearchRequest;
import dev.slsk.internal.search.SearchResponse;
import dev.slsk.internal.search.SearchResult;
import dev.slsk.internal.search.SearchScope;
import dev.slsk.internal.search.SearchState;
import dev.slsk.internal.share.File;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class EngineSearchTest {
    @Test
    void validatesQueryHandlerLoginAndDuplicateToken() {
        Fixture fixture = new Fixture();

        assertThrows(
                NullPointerException.class,
                () -> fixture.client
                        .searches()
                        .search(SearchRequest.of((SearchQuery) null).build()));
        for (String invalid :
                new String[] {"", " ", "\t", "-excluded", "\u00A0", "\u2003", "\u202F", "\u3000", " \u2003\t"}) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> fixture.client
                            .searches()
                            .search(SearchRequest.of(SearchQuery.fromText(invalid))
                                    .build()));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> fixture.client
                        .searches()
                        .search(SearchRequest.of(SearchQuery.fromText("a")).build()));
        assertThrows(
                NullPointerException.class,
                () -> fixture.client
                        .searches()
                        .search(SearchRequest.of(SearchQuery.fromText("valid")).build(), (Consumer<SearchResponse>)
                                null));

        fixture.client.setStateForTest(SoulseekClientState.DISCONNECTED);
        assertThrows(
                IllegalStateException.class,
                () -> fixture.client
                        .searches()
                        .search(SearchRequest.of(SearchQuery.fromText("valid")).build()));

        fixture.client.setStateForTest(loggedIn());
        SearchInternal existing = new SearchInternal(SearchQuery.fromText("existing"), SearchScope.getNetwork(), 42);
        fixture.client.getSearches().put(42, existing);
        assertThrows(
                DuplicateTokenException.class,
                () -> fixture.client
                        .searches()
                        .search(SearchRequest.of(SearchQuery.fromText("valid"))
                                .token(42)
                                .build()));
        fixture.client.getSearches().remove(42);
        existing.close();
        fixture.close();
    }

    @Test
    void tokenCollisionAtInsertionThrowsInsteadOfBeingDiscarded() {
        Fixture fixture = new Fixture();
        SearchInternal existing = new SearchInternal(SearchQuery.fromText("existing"), SearchScope.getNetwork(), 42);
        // A registry whose containsKey lies, simulating a competing search
        // claiming the token between the validate-time check and the insert:
        // the insert is the authoritative claim and must refuse the loser.
        java.util.concurrent.ConcurrentHashMap<Integer, SearchInternal> registry =
                new java.util.concurrent.ConcurrentHashMap<>() {
                    @Override
                    public boolean containsKey(Object key) {
                        return false;
                    }
                };
        registry.put(42, existing);
        fixture.client.setSearchesForTest(registry);

        assertThrows(
                DuplicateTokenException.class,
                () -> fixture.client
                        .searches()
                        .search(SearchRequest.of(SearchQuery.fromText("valid"))
                                .token(42)
                                .options(options(40, 250, true))
                                .build()));
        assertSame(existing, registry.get(42), "the collision evicted the search that owns the token");
        existing.close();
        fixture.close();
    }

    @Test
    void disablingSingleCharacterRemovalSendsSearch() {
        Fixture fixture = new Fixture();
        SearchOptions options = options(40, 250, false);

        SearchResult result = fixture.client
                .searches()
                .search(SearchRequest.of(SearchQuery.fromText("a"))
                        .token(10)
                        .options(options)
                        .build());

        assertArrayEquals(
                new dev.slsk.internal.messaging.messages.SearchRequest("a", 10).toByteArray(),
                fixture.server.messages.get(0));
        assertTrue(result.search().state().contains(SearchState.TIMED_OUT));
        fixture.close();
    }

    @Test
    void sendsFilteredNetworkSearchAndReturnsTimedOutSearch() {
        Fixture fixture = new Fixture();
        List<SearchState> states = new ArrayList<>();
        SearchOptions options =
                options(40, 250, true, change -> states.add(change.search().state()), null);

        SearchResult result = fixture.client
                .searches()
                .search(SearchRequest.of(SearchQuery.fromText("foo a -bar"))
                        .token(11)
                        .options(options)
                        .build());

        assertArrayEquals(
                new dev.slsk.internal.messaging.messages.SearchRequest("foo -bar", 11).toByteArray(),
                fixture.server.messages.get(0));
        assertEquals(
                List.of(
                        SearchState.REQUESTED,
                        SearchState.QUEUED,
                        SearchState.IN_PROGRESS,
                        SearchState.COMPLETED.or(SearchState.TIMED_OUT)),
                states);
        assertEquals("foo -bar", result.search().query().searchText());
        assertEquals(11, result.search().token());
        assertTrue(result.responses().isEmpty());
        assertFalse(fixture.client.getSearches().containsKey(11));
        fixture.close();
    }

    @Test
    void sendsScopeSpecificMessagesIncludingMultipleUsers() {
        assertScopeMessage(SearchScope.getWishlist(), new WishlistSearchRequest("query", 20).toByteArray());
        assertScopeMessage(SearchScope.room("room"), new RoomSearchRequest("room", "query", 20).toByteArray());

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        expected.writeBytes(new UserSearchRequest("alice", "query", 20).toByteArray());
        expected.writeBytes(new UserSearchRequest("bob", "query", 20).toByteArray());
        assertScopeMessage(SearchScope.user("alice", "bob"), expected.toByteArray());
    }

    @Test
    void collectsAcceptedResponseAndRaisesCallbacksAndEvents() {
        Fixture fixture = new Fixture();
        AtomicInteger optionResponses = new AtomicInteger();
        AtomicInteger clientResponses = new AtomicInteger();
        AtomicInteger clientStates = new AtomicInteger();
        fixture.client
                .events()
                .on(Kind.SEARCH_RESPONSE_RECEIVED, (dev.slsk.internal.events.SearchResponseReceivedEvent eventData) -> {
                    assertEquals("alice", eventData.response().username());
                    clientResponses.incrementAndGet();
                });
        fixture.client
                .events()
                .on(
                        Kind.SEARCH_STATE_CHANGED,
                        (dev.slsk.internal.events.SearchStateChangedEvent eventData) -> clientStates.incrementAndGet());
        SearchOptions options = options(2_000, 1, true, null, received -> optionResponses.incrementAndGet());

        CompletableFuture<SearchResult> task = inBackground(() -> fixture.client
                .searches()
                .search(SearchRequest.of(SearchQuery.fromText("query"))
                        .token(30)
                        .options(options)
                        .build()));
        waitUntil(() -> {
            SearchInternal active = fixture.client.getSearches().get(30);
            return active != null && active.getState().equals(SearchState.IN_PROGRESS);
        });
        SearchResponse response =
                new SearchResponse("alice", 30, true, 100, 0, List.of(new File(2, "file.mp3", 3, "mp3")));
        fixture.client.getSearches().get(30).tryAddResponse(response);

        SearchResult result = task.join();
        assertEquals(1, result.responses().size());
        assertEquals(response.username(), result.responses().getFirst().username());
        assertEquals(response.token(), result.responses().getFirst().token());
        assertEquals(1, result.search().responseCount());
        assertEquals(1, result.search().fileCount());
        assertEquals(
                SearchState.COMPLETED.or(SearchState.RESPONSE_LIMIT_REACHED),
                result.search().state());
        assertEquals(1, optionResponses.get());
        assertEquals(1, clientResponses.get());
        assertEquals(4, clientStates.get());
        assertThrows(
                UnsupportedOperationException.class, () -> result.responses().add(response));
        fixture.close();
    }

    @Test
    void callbackOverloadReceivesResponseAndReturnsSnapshot() {
        Fixture fixture = new Fixture();
        List<SearchResponse> responses = new ArrayList<>();
        SearchOptions options = options(2_000, 1, true);

        CompletableFuture<Search> task = inBackground(() -> fixture.client
                .searches()
                .search(
                        SearchRequest.of(SearchQuery.fromText("query"))
                                .scope(SearchScope.getNetwork())
                                .token(31)
                                .options(options)
                                .build(),
                        responses::add));
        waitUntil(() -> fixture.client.getSearches().containsKey(31)
                && fixture.client.getSearches().get(31).getState().equals(SearchState.IN_PROGRESS));
        SearchResponse response = new SearchResponse("bob", 31, true, 1, 0, List.of(new File(2, "file", 3, "ext")));
        fixture.client.getSearches().get(31).tryAddResponse(response);

        Search search = task.join();
        assertEquals(1, responses.size());
        assertEquals(response.username(), responses.getFirst().username());
        assertEquals(response.token(), responses.getFirst().token());
        assertEquals(31, search.token());
        assertEquals(1, search.responseCount());
        fixture.close();
    }

    @Test
    void generatesTokenAndTracksActiveSearch() {
        Fixture fixture = new Fixture();
        CancellationController source = new CancellationController();
        CompletableFuture<SearchResult> task = inBackground(() -> fixture.client
                .searches()
                .search(SearchRequest.of(SearchQuery.fromText("query"))
                        .options(options(2_000, 250, true))
                        .cancellation(source.getSignal())
                        .build()));

        waitUntil(() -> fixture.client.getSearches().size() == 1);
        SearchInternal active = fixture.client.getSearches().values().iterator().next();
        assertEquals("query", active.getQuery().searchText());
        source.cancel();
        assertInstanceOf(CancellationException.class, completionCause(task::join));
        assertTrue(fixture.client.getSearches().isEmpty());
        fixture.close();
    }

    @Test
    void preservesCancellationAndWriteTimeoutButWrapsOtherErrors() {
        Fixture fixture = new Fixture();
        CancellationController source = new CancellationController();
        source.cancel();
        CompletableFuture<SearchResult> cancelled = inBackground(() -> fixture.client
                .searches()
                .search(SearchRequest.of(SearchQuery.fromText("cancelled"))
                        .token(40)
                        .options(options(2_000, 250, true))
                        .cancellation(source.getSignal())
                        .build()));
        assertInstanceOf(CancellationException.class, completionCause(() -> cancelled.join()));

        TimeoutException timeout = new TimeoutException("write");
        fixture.server.result = CompletableFuture.failedFuture(timeout);
        CompletableFuture<SearchResult> timedOut = inBackground(() -> fixture.client
                .searches()
                .search(SearchRequest.of(SearchQuery.fromText("timeout"))
                        .token(41)
                        .options(options(2_000, 250, true))
                        .build()));
        assertSame(
                timeout,
                assertInstanceOf(NoResponseException.class, completionCause(() -> timedOut.join()))
                        .getCause());

        IllegalStateException error = new IllegalStateException("boom");
        fixture.server.result = CompletableFuture.failedFuture(error);
        CompletableFuture<SearchResult> failed = inBackground(() -> fixture.client
                .searches()
                .search(SearchRequest.of(SearchQuery.fromText("error"))
                        .token(42)
                        .options(options(2_000, 250, true))
                        .build()));
        SoulseekClientException wrapped =
                assertInstanceOf(SoulseekClientException.class, completionCause(() -> failed.join()));
        assertSame(error, wrapped.getCause());
        assertTrue(wrapped.getMessage().contains("error (42)"));
        fixture.close();
    }

    @Test
    void limitsConcurrentSearchesAndReleasesQueuedSearch() {
        Fixture fixture = new Fixture();
        SearchOptions options = options(2_000, 250, true);
        CancellationController firstSource = new CancellationController();
        CancellationController secondSource = new CancellationController();
        CancellationController thirdSource = new CancellationController();
        // Started one at a time, each waited on before the next. The callers run
        // on independent threads now, so without this the two that win the
        // concurrency semaphore are whichever two get scheduled first — and the
        // test would be asserting on a race.
        CompletableFuture<SearchResult> first = inBackground(() -> fixture.client
                .searches()
                .search(SearchRequest.of(SearchQuery.fromText("one"))
                        .token(51)
                        .options(options)
                        .cancellation(firstSource.getSignal())
                        .build()));
        waitUntil(() -> fixture.server.messages.size() == 1);
        CompletableFuture<SearchResult> second = inBackground(() -> fixture.client
                .searches()
                .search(SearchRequest.of(SearchQuery.fromText("two"))
                        .token(52)
                        .options(options)
                        .cancellation(secondSource.getSignal())
                        .build()));
        waitUntil(() -> fixture.server.messages.size() == 2);
        CompletableFuture<SearchResult> third = inBackground(() -> fixture.client
                .searches()
                .search(SearchRequest.of(SearchQuery.fromText("three"))
                        .token(53)
                        .options(options)
                        .cancellation(thirdSource.getSignal())
                        .build()));

        // Registration and the QUEUED transition are separate steps, and the
        // caller is on its own thread, so wait for the state rather than for
        // the entry to appear.
        waitUntil(() -> fixture.client.getSearches().get(53) != null
                && fixture.client.getSearches().get(53).getState() == SearchState.QUEUED);
        assertEquals(SearchState.QUEUED, fixture.client.getSearches().get(53).getState());
        firstSource.cancel();
        assertInstanceOf(CancellationException.class, completionCause(() -> first.join()));
        waitUntil(() -> fixture.server.messages.size() == 3);
        assertArrayEquals(
                new dev.slsk.internal.messaging.messages.SearchRequest("three", 53).toByteArray(),
                fixture.server.messages.get(2));
        secondSource.cancel();
        thirdSource.cancel();
        assertInstanceOf(CancellationException.class, completionCause(() -> second.join()));
        assertInstanceOf(CancellationException.class, completionCause(() -> third.join()));
        fixture.close();
    }

    private static void assertScopeMessage(SearchScope scope, byte[] expected) {
        Fixture fixture = new Fixture();
        SearchResult result = fixture.client
                .searches()
                .search(SearchRequest.of(SearchQuery.fromText("query"))
                        .scope(scope)
                        .token(20)
                        .options(options(30, 250, true))
                        .build());
        assertArrayEquals(expected, fixture.server.messages.get(0));
        assertTrue(result.search().state().contains(SearchState.TIMED_OUT));
        fixture.close();
    }

    private static SearchOptions options(int timeout, int responseLimit, boolean removeSingleCharacterTerms) {
        return options(timeout, responseLimit, removeSingleCharacterTerms, null, null);
    }

    private static SearchOptions options(
            int timeout,
            int responseLimit,
            boolean removeSingleCharacterTerms,
            Consumer<SearchStateChange> stateChanged,
            Consumer<SearchResponseReceived> responseReceived) {
        return SearchOptions.builder()
                .searchTimeout(java.time.Duration.ofMillis(timeout))
                .responseLimit(responseLimit)
                .removeSingleCharacterSearchTerms(removeSingleCharacterTerms)
                .stateChanged(stateChanged)
                .responseReceived(responseReceived)
                .build();
    }

    private static SoulseekClientState loggedIn() {
        return SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGED_IN);
    }

    /**
     * Runs a blocking client call on a virtual thread so the test can interact
     * with it while it is in flight.
     *
     * <p>The API used to hand back a future; now the caller decides whether to
     * be concurrent, and a test that wants to inject a response mid-call is
     * exactly such a caller. The assertions around it are unchanged.
     */
    private static <T> CompletableFuture<T> inBackground(java.util.function.Supplier<T> call) {
        return CompletableFuture.supplyAsync(call, Executors.newVirtualThreadPerTaskExecutor());
    }

    /** Void-returning variant of {@link #inBackground}. */
    private static CompletableFuture<Void> inBackground(Runnable call) {
        return CompletableFuture.runAsync(call, Executors.newVirtualThreadPerTaskExecutor());
    }

    /**
     * Returns the failure a blocking call produced.
     *
     * <p>Took a future before the API became blocking; the calls now throw
     * directly, so it takes the call itself.
     */
    private static Throwable completionCause(org.junit.jupiter.api.function.Executable body) {
        try {
            body.execute();
        } catch (java.util.concurrent.CompletionException wrapped) {
            return wrapped.getCause() == null ? wrapped : wrapped.getCause();
        } catch (Throwable failure) {
            return failure;
        }
        throw new AssertionError("expected the operation to fail");
    }

    private static void waitUntil(BooleanSupplier condition) {
        long deadline = System.nanoTime() + 2_000_000_000L;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("Condition was not met");
            }
            try {
                Thread.sleep(2);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new AssertionError(failure);
            }
        }
    }

    private static Object defaultValue(Class<?> type) {
        if (type == CompletableFuture.class) {
            return CompletableFuture.completedFuture(null);
        }
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
        if (type == double.class) {
            return 0D;
        }
        return null;
    }

    private static final class Fixture {
        private final ConnectionProbe server = new ConnectionProbe();
        private final SoulseekEngine client = new SoulseekEngine(
                9999,
                null,
                server.proxy,
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
                null,
                null);

        private Fixture() {
            client.setStateForTest(loggedIn());
        }

        private void close() {
            client.close();
        }
    }

    private static final class ConnectionProbe {
        private final List<byte[]> messages = java.util.Collections.synchronizedList(new ArrayList<>());
        private volatile CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            if (method.getName().equals("write") && arguments.length == 2 && arguments[0] instanceof byte[] bytes) {
                messages.add(bytes);
                Outcomes.raise(result);
                return null;
            }
            return defaultValue(method.getReturnType());
        }
    }
}
