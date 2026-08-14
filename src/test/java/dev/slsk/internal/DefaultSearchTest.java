// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationController;
import dev.slsk.CancellationSignal;
import dev.slsk.events.SearchEvent;
import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.diagnostics.DiagnosticLevel;
import dev.slsk.internal.diagnostics.FilteringDiagnosticSink;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.search.SearchId;
import dev.slsk.search.SearchLimits;
import dev.slsk.search.SearchQuery;
import dev.slsk.search.SearchResult;
import dev.slsk.search.SearchSnapshot;
import dev.slsk.search.SearchStatus;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The search facet's lifecycle, which the shipped 1.0.3 got wrong in four ways.
 *
 * <p>These run against a logged-in engine over a probe connection, so a search
 * is genuinely in flight — it has been written to the server and is waiting out
 * its timeout — for as long as the test needs it to be. Asserting any of this
 * against a disconnected client would pass whether or not the fixes are there,
 * because every search would fail instantly.
 */
class DefaultSearchTest {

    /** Long enough that nothing finishes on its own inside a test. */
    private static final Duration NEVER = Duration.ofSeconds(30);

    @Test
    @DisplayName("start returns while the search is still running, rather than blocking for it")
    void startReturnsImmediately() {
        try (Fixture fixture = new Fixture()) {
            SearchId id = fixture.search.start(query(NEVER));

            assertEquals(SearchStatus.IN_PROGRESS, fixture.search.get(id).status());
            assertEquals(
                    List.of(id),
                    fixture.search.active().stream().map(SearchSnapshot::id).toList());
            // It reached the wire without the caller waiting for the result.
            fixture.waitUntil(() -> !fixture.server.messages.isEmpty());
        }
    }

    @Test
    @DisplayName("a search started with start can be stopped")
    void startedSearchesAreCancellable() {
        try (Fixture fixture = new Fixture()) {
            SearchId id = fixture.search.start(query(NEVER));
            fixture.waitUntil(() -> !fixture.server.messages.isEmpty());

            fixture.search.stop(id);

            assertEquals(SearchStatus.CANCELLED, fixture.search.get(id).status());
            // Not merely relabelled: the engine dropped the search, which only
            // happens when the operation itself ended.
            fixture.waitUntil(() -> fixture.client.getSearches().isEmpty());
            assertEquals(List.of(), fixture.search.active());
        }
    }

    @Test
    @DisplayName("await waits for the search to reach a terminal status")
    void awaitWaits() throws InterruptedException {
        try (Fixture fixture = new Fixture()) {
            SearchId id = fixture.search.start(query(NEVER));
            fixture.waitUntil(() -> !fixture.server.messages.isEmpty());

            CountDownLatch returned = new CountDownLatch(1);
            List<SearchResult> results = Collections.synchronizedList(new ArrayList<>());
            Thread waiter = Thread.ofVirtual().start(() -> {
                results.add(fixture.search.await(id, CancellationSignal.none()));
                returned.countDown();
            });

            // The search has not finished, so neither has the wait.
            assertFalse(returned.await(150, TimeUnit.MILLISECONDS));

            fixture.search.stop(id);

            assertTrue(returned.await(5, TimeUnit.SECONDS), "await did not return once the search stopped");
            waiter.join();
            assertEquals(SearchStatus.CANCELLED, results.get(0).status());
        }
    }

    @Test
    @DisplayName("await on a finished search returns without waiting")
    void awaitOnAFinishedSearchReturns() {
        try (Fixture fixture = new Fixture()) {
            SearchId id = fixture.search.start(query(NEVER));
            fixture.search.stop(id);

            SearchResult result = fixture.search.await(id, CancellationSignal.none());

            assertEquals(SearchStatus.CANCELLED, result.status());
            assertEquals(id, result.id());
        }
    }

    @Test
    @DisplayName("the signal passed to await stops the search, as the facet documents")
    void awaitSignalStopsTheSearch() {
        try (Fixture fixture = new Fixture();
                CancellationController controller = new CancellationController()) {
            SearchId id = fixture.search.start(query(NEVER));
            fixture.waitUntil(() -> !fixture.server.messages.isEmpty());

            Thread.ofVirtual().start(() -> {
                sleep(50);
                controller.cancel();
            });
            SearchResult result = fixture.search.await(id, controller.getSignal());

            assertEquals(SearchStatus.CANCELLED, result.status());
            assertEquals(SearchStatus.CANCELLED, fixture.search.get(id).status());
        }
    }

    @Test
    @DisplayName("a search cancelled through run's own signal is cancelled, not timed out")
    void runIsCancellableThroughItsSignal() {
        try (Fixture fixture = new Fixture();
                CancellationController controller = new CancellationController()) {
            Thread.ofVirtual().start(() -> {
                sleep(100);
                controller.cancel();
            });

            SearchResult result = fixture.search.run(query(NEVER), controller.getSignal());

            assertEquals(SearchStatus.CANCELLED, result.status());
        }
    }

    @Test
    @DisplayName("stopping a search reaches run, which is blocked on it")
    void stopReachesARunningSearch() throws InterruptedException {
        try (Fixture fixture = new Fixture()) {
            CountDownLatch returned = new CountDownLatch(1);
            List<SearchResult> results = Collections.synchronizedList(new ArrayList<>());
            Thread.ofVirtual().start(() -> {
                results.add(fixture.search.run(query(NEVER), CancellationSignal.none()));
                returned.countDown();
            });

            fixture.waitUntil(() -> !fixture.search.active().isEmpty());
            SearchId id = fixture.search.active().get(0).id();
            fixture.search.stop(id);

            assertTrue(returned.await(5, TimeUnit.SECONDS), "run did not return once the search stopped");
            assertEquals(SearchStatus.CANCELLED, results.get(0).status());
        }
    }

    @Test
    @DisplayName("one terminal transition is published however many callers ask for it")
    void oneTerminalEventPerSearch() {
        try (Fixture fixture = new Fixture()) {
            List<SearchEvent.StatusChanged> terminal = Collections.synchronizedList(new ArrayList<>());
            fixture.events.subscribe(SearchEvent.StatusChanged.class, event -> {
                if (event.to().isTerminal()) {
                    terminal.add(event);
                }
            });

            SearchId id = fixture.search.start(query(NEVER));
            fixture.waitUntil(() -> !fixture.server.messages.isEmpty());
            fixture.search.stop(id);
            fixture.search.stop(id);
            // The first terminal event arrives asynchronously on the delivery
            // thread; wait for it deterministically before opening the window
            // in which a duplicate would have to show up.
            fixture.waitUntil(() -> !terminal.isEmpty());
            // The operation this stopped also finishes, and must not publish a
            // second terminal event for the same search.
            sleep(200);

            assertEquals(1, terminal.size());
            assertEquals(SearchStatus.CANCELLED, terminal.get(0).to());
        }
    }

    @Test
    @DisplayName("finished searches are retained only up to the bound")
    void finishedSearchesAreBounded() {
        try (Fixture fixture = new Fixture()) {
            int overflow = 5;
            List<SearchId> ids = new ArrayList<>();
            for (int index = 0; index < DefaultSearch.RETAINED_FINISHED_SEARCHES + overflow; index++) {
                SearchId id = fixture.search.start(query(NEVER));
                fixture.search.stop(id);
                ids.add(id);
            }

            for (int index = 0; index < overflow; index++) {
                SearchId dropped = ids.get(index);
                assertThrows(IllegalArgumentException.class, () -> fixture.search.get(dropped));
            }
            assertEquals(
                    SearchStatus.CANCELLED,
                    fixture.search.get(ids.get(ids.size() - 1)).status());
        }
    }

    @Test
    @DisplayName("a running search is never dropped by retention")
    void runningSearchesSurviveRetention() {
        try (Fixture fixture = new Fixture()) {
            SearchId running = fixture.search.start(query(NEVER));
            for (int index = 0; index < DefaultSearch.RETAINED_FINISHED_SEARCHES + 5; index++) {
                fixture.search.stop(fixture.search.start(query(NEVER)));
            }

            assertEquals(SearchStatus.IN_PROGRESS, fixture.search.get(running).status());
        }
    }

    @Test
    void awaitAndGetRejectUnknownSearches() {
        try (Fixture fixture = new Fixture()) {
            SearchId unknown = SearchId.ofToken(987654);
            assertThrows(IllegalArgumentException.class, () -> fixture.search.get(unknown));
            assertThrows(
                    IllegalArgumentException.class, () -> fixture.search.await(unknown, CancellationSignal.none()));
        }
    }

    private static SearchQuery query(Duration overall) {
        return SearchQuery.of("something").withLimits(new SearchLimits(overall, overall, 250, 250));
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    /** A logged-in engine over a probe connection, with the facet on top of it. */
    private static final class Fixture implements AutoCloseable {
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
        private final EventBus<SearchEvent> events =
                new EventBus<>("search", new FilteringDiagnosticSink(DiagnosticLevel.NONE, event -> {}));
        private final DefaultSearch search = new DefaultSearch(client, events);

        private Fixture() {
            client.setStateForTest(SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGED_IN));
        }

        private void waitUntil(BooleanSupplier condition) {
            long deadline = System.nanoTime() + 5_000_000_000L;
            while (!condition.getAsBoolean()) {
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("Condition was not met");
                }
                sleep(2);
            }
        }

        @Override
        public void close() {
            search.close();
            client.close();
        }
    }

    private static final class ConnectionProbe {
        private final List<byte[]> messages = Collections.synchronizedList(new ArrayList<>());
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("write") && arguments.length == 2 && arguments[0] instanceof byte[] bytes) {
                messages.add(bytes);
                return null;
            }
            return defaultValue(method.getReturnType());
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
    }
}
