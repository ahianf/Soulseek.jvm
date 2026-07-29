// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.exceptions.ConnectionException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The per-peer cache entry, on its own.
 *
 * <p>Every test runs on a separate thread, so a regression that never releases
 * the waiters fails in ten seconds rather than hanging the build.
 */
@Timeout(value = 10, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ConnectionCellTest {

    @Test
    @DisplayName("one connection is handed to every waiter")
    void oneConnectionIsBroadcastToEveryWaiter() {
        ConnectionCell cell = new ConnectionCell();
        MessageConnection connection = connection();
        List<CompletableFuture<MessageConnection>> waiters = await(cell, 8);

        cell.settle(connection);

        waiters.forEach(waiter -> assertSame(connection, waiter.join()));
    }

    @Test
    @DisplayName("one failure is raised to every waiter, in the shape join produced")
    void oneFailureIsBroadcastToEveryWaiter() {
        ConnectionCell cell = new ConnectionCell();
        ConnectionException cause = new ConnectionException("no route to host");
        List<CompletableFuture<MessageConnection>> waiters = await(cell, 8);

        cell.fail(cause);

        for (CompletableFuture<MessageConnection> waiter : waiters) {
            CompletionException thrown = assertThrows(CompletionException.class, waiter::join);
            assertSame(cause, thrown.getCause());
        }
        assertNull(cell.awaitQuietly());
    }

    @Test
    @DisplayName("a cancellation is raised as itself")
    void cancellationIsRaisedUnwrapped() {
        ConnectionCell cell = new ConnectionCell();
        cell.fail(new CancellationException("cancelled"));

        assertThrows(CancellationException.class, cell::await);
        assertNull(cell.awaitQuietly());
    }

    /**
     * What counting the connections to peers relies on. An establishment in
     * flight is not a connection, and asking about it must not wait for it.
     */
    @Test
    @DisplayName("peeking never waits and never sees an unsettled or failed cell")
    void peekingNeverWaits() {
        ConnectionCell established = new ConnectionCell();
        assertNull(established.peek());
        MessageConnection connection = connection();
        established.settle(connection);
        assertSame(connection, established.peek());

        ConnectionCell failed = new ConnectionCell();
        failed.fail(new ConnectionException("refused"));
        assertNull(failed.peek());
    }

    @Test
    @DisplayName("a cell settles once, and the loser of that race changes nothing")
    void aCellSettlesOnlyOnce() {
        ConnectionCell settled = new ConnectionCell();
        MessageConnection first = connection();
        settled.settle(first);
        settled.settle(connection());
        settled.fail(new ConnectionException("late"));
        assertSame(first, settled.await());

        ConnectionCell failed = new ConnectionCell();
        ConnectionException cause = new ConnectionException("first");
        failed.fail(cause);
        failed.fail(new ConnectionException("second"));
        failed.settle(connection());
        assertSame(cause, assertThrows(CompletionException.class, failed::await).getCause());
    }

    /**
     * Disposing of the cache has to reach the attempts still in it: an
     * establishment in flight owns a socket nobody wants any more, and it
     * cannot be closed until it exists.
     */
    @Test
    @DisplayName("a disposed cell closes its connection whenever it arrives")
    void aDisposedCellClosesWhateverSettles() {
        ConnectionCell inFlight = new ConnectionCell();
        ConnectionProbe pending = new ConnectionProbe();
        inFlight.closeWhenSettled();
        assertEquals(0, pending.closeCount.get());

        inFlight.settle(pending.connection());
        assertEquals(1, pending.closeCount.get());

        ConnectionCell established = new ConnectionCell();
        ConnectionProbe connected = new ConnectionProbe();
        established.settle(connected.connection());
        established.closeWhenSettled();
        assertEquals(1, connected.closeCount.get());

        // A failed attempt disposed of its own connection on the way out, so
        // there is nothing left here for the cache to close.
        ConnectionCell failed = new ConnectionCell();
        failed.fail(new ConnectionException("refused"));
        assertDoesNotThrow(failed::closeWhenSettled);
    }

    private static List<CompletableFuture<MessageConnection>> await(ConnectionCell cell, int waiters) {
        CountDownLatch waiting = new CountDownLatch(waiters);
        List<CompletableFuture<MessageConnection>> awaits = new ArrayList<>();
        for (int waiter = 0; waiter < waiters; waiter++) {
            awaits.add(CompletableFuture.supplyAsync(
                    () -> {
                        waiting.countDown();
                        return cell.await();
                    },
                    task -> Thread.ofVirtual().start(task)));
        }
        try {
            assertTrue(waiting.await(10, TimeUnit.SECONDS), "the waiters never started");
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new AssertionError(exception);
        }
        return awaits;
    }

    private static MessageConnection connection() {
        return new ConnectionProbe().connection();
    }

    private static final class ConnectionProbe implements InvocationHandler {
        private final AtomicInteger closeCount = new AtomicInteger();
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this);

        private MessageConnection connection() {
            return proxy;
        }

        @Override
        public Object invoke(Object ignored, Method method, Object[] arguments) {
            return switch (method.getName()) {
                case "close" -> {
                    closeCount.incrementAndGet();
                    yield null;
                }
                case "toString" -> "ConnectionProbe";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == arguments[0];
                default -> method.getReturnType().isPrimitive() ? 0 : null;
            };
        }
    }
}
