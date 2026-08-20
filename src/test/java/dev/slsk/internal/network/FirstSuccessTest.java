// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.network.tcp.TransportConnection;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The direct/indirect race, on its own.
 *
 * <p>Every test here runs on a separate thread, so a regression that stops the
 * handoff from ever being offered fails in ten seconds instead of hanging the
 * build.
 */
@Timeout(value = 10, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FirstSuccessTest {

    @Test
    @DisplayName("the first arm to succeed wins, and says which arm it was")
    void theFirstArmToSucceedWins() throws Exception {
        ConnectionProbe direct = new ConnectionProbe();
        FirstSuccess.Winner<TransportConnection> directWinner =
                FirstSuccess.race(direct::connection, () -> refuse("indirect"));
        assertSame(direct.connection(), directWinner.value());
        assertTrue(directWinner.first());

        ConnectionProbe indirect = new ConnectionProbe();
        FirstSuccess.Winner<TransportConnection> indirectWinner =
                FirstSuccess.race(() -> refuse("direct"), indirect::connection);
        assertSame(indirect.connection(), indirectWinner.value());
        assertFalse(indirectWinner.first());
    }

    /**
     * The property the helper exists for. A peer behind a firewall never
     * answers the direct attempt and a peer that has dropped off the server
     * never answers the indirect one; either failure alone is ordinary, and the
     * arm that is still trying must be allowed to win.
     */
    @Test
    @DisplayName("an arm that fails does not lose the race for the one still trying")
    void oneArmFailingDoesNotLoseTheRace() throws Exception {
        ConnectionProbe slow = new ConnectionProbe();
        CountDownLatch directFailed = new CountDownLatch(1);

        FirstSuccess.Winner<TransportConnection> winner = FirstSuccess.race(
                () -> {
                    directFailed.countDown();
                    return refuse("direct");
                },
                () -> {
                    await(directFailed);
                    return slow.connection();
                });

        assertSame(slow.connection(), winner.value());
        assertFalse(winner.first());
    }

    @Test
    @DisplayName("the race is lost only when both arms fail, and raises the failure that ended it")
    void bothArmsFailingLosesTheRace() throws InterruptedException {
        CountDownLatch directFailed = new CountDownLatch(1);
        CountDownLatch releaseIndirect = new CountDownLatch(1);
        CountDownLatch raceEnded = new CountDownLatch(1);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        IllegalStateException lastToFail = new IllegalStateException("indirect");

        Thread.ofVirtual().start(() -> {
            try {
                FirstSuccess.race(
                        () -> {
                            directFailed.countDown();
                            return refuse("direct");
                        },
                        () -> {
                            await(releaseIndirect);
                            throw lastToFail;
                        });
            } catch (Throwable failure) {
                thrown.set(failure);
            } finally {
                raceEnded.countDown();
            }
        });

        await(directFailed);
        assertFalse(
                raceEnded.await(100, TimeUnit.MILLISECONDS),
                "one arm failing must not end the race while the other is still trying");

        releaseIndirect.countDown();
        assertTrue(raceEnded.await(10, TimeUnit.SECONDS), "the race never ended");
        assertSame(lastToFail, thrown.get(), "the losing failure arrives as itself, unwrapped");
    }

    /**
     * Why a loser needs closing at all: both arms can succeed. The peer answers
     * the direct attempt while the server is still relaying the indirect
     * solicitation, and the second connection to arrive is a live socket that
     * nobody will ever read from. Before this helper it was simply dropped.
     */
    @Test
    @DisplayName("the arm that arrives second is closed")
    void theLoserIsClosed() throws Exception {
        ConnectionProbe winner = new ConnectionProbe();
        ConnectionProbe loser = new ConnectionProbe();
        CountDownLatch raceDecided = new CountDownLatch(1);

        FirstSuccess.Winner<TransportConnection> outcome = FirstSuccess.race(winner::connection, () -> {
            await(raceDecided);
            return loser.connection();
        });

        assertSame(winner.connection(), outcome.value());
        raceDecided.countDown();
        assertTrue(loser.awaitClose(), "the losing arm's connection should have been closed");
        assertEquals(0, winner.closeCount());
    }

    /**
     * A cancellation is a decision the caller made, and comes back as itself —
     * the shape {@code join()} presented and every call site reads.
     */
    @Test
    @DisplayName("a cancelled race raises the cancellation itself")
    void cancellationIsNotWrappedInACompletionException() {
        CountDownLatch directFailed = new CountDownLatch(1);

        assertThrows(
                CancellationException.class,
                () -> FirstSuccess.race(
                        () -> {
                            directFailed.countDown();
                            throw new CancellationException("direct");
                        },
                        () -> {
                            await(directFailed);
                            throw new CancellationException("indirect");
                        }));
    }

    /** What an arm does when it cannot reach the peer, wrappers and all. */
    private static TransportConnection refuse(String arm) {
        throw new CompletionException(new IllegalStateException(arm));
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(10, TimeUnit.SECONDS), "the arm was never released");
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new AssertionError(exception);
        }
    }

    private static final class ConnectionProbe implements InvocationHandler {
        private final AtomicInteger closeCount = new AtomicInteger();
        private final CountDownLatch closed = new CountDownLatch(1);
        private final TransportConnection proxy = (TransportConnection) Proxy.newProxyInstance(
                TransportConnection.class.getClassLoader(), new Class<?>[] {TransportConnection.class}, this);

        private TransportConnection connection() {
            return proxy;
        }

        private int closeCount() {
            return closeCount.get();
        }

        private boolean awaitClose() {
            try {
                return closed.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new AssertionError(exception);
            }
        }

        @Override
        public Object invoke(Object ignored, Method method, Object[] arguments) {
            return switch (method.getName()) {
                case "close" -> {
                    closeCount.incrementAndGet();
                    closed.countDown();
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
