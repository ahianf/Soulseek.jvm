// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.connection.ConnectionState;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.LoginRejectedException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What gets the connection back, and — more to the point — what stops it trying.
 *
 * <p>The delays are handed in rather than taken from the production constants,
 * so these assert the loop's shape without spending real seconds asleep. The
 * ceiling sequence itself is asserted separately, against the real constants.
 */
class ReconnectSupervisorTest {

    /** Fast enough that a test is not a stopwatch, slow enough to be observable. */
    private static final Duration QUICK = Duration.ofMillis(10);

    private static final Duration QUICK_FLOOR = Duration.ofMillis(1);

    private static ReconnectSupervisor supervisor(ReconnectSupervisor.Connector connector, Runnable onStateChanged) {
        return new ReconnectSupervisor(connector, onStateChanged, QUICK, QUICK, QUICK_FLOOR);
    }

    @Test
    @DisplayName("keeps trying until a connect succeeds, then stops")
    void retriesUntilOnline() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch online = new CountDownLatch(1);
        try (ReconnectSupervisor supervisor = supervisor(
                () -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw new ConnectionException("still down");
                    }
                    online.countDown();
                },
                () -> {})) {

            supervisor.arm(new ConnectionException("the network went away"));

            assertTrue(online.await(5, TimeUnit.SECONDS), "never reconnected");
            // Settle, then confirm it really stopped rather than merely having
            // succeeded once on its way to trying forever.
            Thread.sleep(100);
            assertEquals(3, attempts.get());
            assertNull(supervisor.pending());
        }
    }

    @Test
    @DisplayName("a rejected login is terminal: retrying a wrong password is abuse")
    void stopsWhenTheServerRejectsTheLogin() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        try (ReconnectSupervisor supervisor = supervisor(
                () -> {
                    attempts.incrementAndGet();
                    throw new LoginRejectedException("INVALIDPASS");
                },
                () -> {})) {

            supervisor.arm(new ConnectionException("dropped"));

            Thread.sleep(200);
            assertEquals(1, attempts.get(), "a rejected login must not be retried");
        }
    }

    @Test
    @DisplayName("reports Reconnecting while it waits, carrying why and when")
    void publishesReconnectingWhileWaiting() throws Exception {
        ConnectionException cause = new ConnectionException("read error");
        CountDownLatch waiting = new CountDownLatch(1);
        List<ConnectionState.Reconnecting> seen = new CopyOnWriteArrayList<>();

        // A slow floor so the waiting state is observable before the attempt.
        ReconnectSupervisor supervisor = new ReconnectSupervisor(
                () -> {
                    throw new ConnectionException("still down");
                },
                () -> {},
                Duration.ofMillis(400),
                Duration.ofMillis(400),
                Duration.ofMillis(400));
        try (supervisor) {
            Thread watcher = Thread.ofVirtual().start(() -> {
                while (waiting.getCount() > 0) {
                    ConnectionState.Reconnecting pending = supervisor.pending();
                    if (pending != null) {
                        seen.add(pending);
                        waiting.countDown();
                        return;
                    }
                    Thread.onSpinWait();
                }
            });
            supervisor.arm(cause);
            assertTrue(waiting.await(5, TimeUnit.SECONDS), "never reported Reconnecting");
            watcher.join();
        }

        ConnectionState.Reconnecting first = seen.get(0);
        assertEquals(2, first.attempt(), "the first retry is attempt two; the drop was attempt one");
        assertSame(cause, first.lastFailure());
        assertNotNull(first.nextAttemptAt());
    }

    @Test
    @DisplayName("cancel stops retrying: the consumer's intent outranks ours")
    void cancelStopsRetrying() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        try (ReconnectSupervisor supervisor = supervisor(
                () -> {
                    attempts.incrementAndGet();
                    throw new ConnectionException("still down");
                },
                () -> {})) {

            supervisor.arm(new ConnectionException("dropped"));
            Thread.sleep(120);
            supervisor.cancel();

            int afterCancel = attempts.get();
            Thread.sleep(200);
            assertEquals(afterCancel, attempts.get(), "kept trying after cancel");
            assertNull(supervisor.pending());
            assertEquals(1, supervisor.attempt(), "cancel resets the attempt counter");
        }
    }

    @Test
    @DisplayName("arming twice does not start two loops")
    void armingIsIdempotentWhileRunning() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch started = new CountDownLatch(1);
        try (ReconnectSupervisor supervisor = new ReconnectSupervisor(
                () -> {
                    attempts.incrementAndGet();
                    started.countDown();
                    throw new ConnectionException("still down");
                },
                () -> {},
                Duration.ofMillis(60),
                Duration.ofMillis(60),
                Duration.ofMillis(60))) {

            // A failed attempt raises its own disconnect, which arrives back as
            // another arm while the loop that caused it is still running.
            supervisor.arm(new ConnectionException("dropped"));
            supervisor.arm(new ConnectionException("dropped again"));
            supervisor.arm(new ConnectionException("and again"));

            assertTrue(started.await(5, TimeUnit.SECONDS));
            Thread.sleep(150);
            // One loop at ~60ms makes two or three attempts in 150ms; three
            // loops would make far more. The bound is loose on purpose.
            assertTrue(attempts.get() <= 4, "more than one retry loop is running: " + attempts.get());
        }
    }

    @Test
    @DisplayName("a cancelled loop cannot ride a later arm: one loop at a time")
    void cancelledLoopDoesNotRideALaterArm() throws Exception {
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        java.util.Set<Thread> loopsAfterRelease = java.util.concurrent.ConcurrentHashMap.newKeySet();

        try (ReconnectSupervisor supervisor = supervisor(
                () -> {
                    if (calls.getAndIncrement() == 0) {
                        firstEntered.countDown();
                        // Swallow the cancel's interrupt, the way a connector
                        // built on non-interruptible I/O can: the loop survives
                        // its own cancel and must still not outlive it.
                        boolean waiting = true;
                        while (waiting) {
                            try {
                                release.await();
                                waiting = false;
                            } catch (InterruptedException swallowed) {
                                // Deliberately not restored.
                            }
                        }
                        throw new ConnectionException("released");
                    }
                    loopsAfterRelease.add(Thread.currentThread());
                    throw new ConnectionException("still down");
                },
                () -> {})) {

            supervisor.arm(new ConnectionException("dropped"));
            assertTrue(firstEntered.await(5, TimeUnit.SECONDS), "first loop never attempted");

            supervisor.cancel();
            supervisor.arm(new ConnectionException("dropped again"));
            release.countDown();

            // Long enough for the stale loop, were it still riding the flag,
            // to sleep its 10 ms delay and call the connector several times.
            Thread.sleep(300);
            assertEquals(1, loopsAfterRelease.size(), "a cancelled loop kept retrying beside the armed one");
            assertTrue(supervisor.retrying(), "the armed loop was stopped by the stale one's exit");
        }
    }

    @Test
    @DisplayName("close stops the retry loop and leaves nothing running")
    void closeStopsTheThread() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch started = new CountDownLatch(1);
        ReconnectSupervisor supervisor = supervisor(
                () -> {
                    attempts.incrementAndGet();
                    started.countDown();
                    throw new ConnectionException("still down");
                },
                () -> {});

        supervisor.arm(new ConnectionException("dropped"));
        assertTrue(started.await(5, TimeUnit.SECONDS));
        supervisor.close();

        assertNull(supervisor.pending());
        assertFalse(supervisor.retrying(), "a retry loop outlived close()");
        int afterClose = attempts.get();
        Thread.sleep(150);
        assertEquals(afterClose, attempts.get(), "kept trying after close");
    }

    @Test
    void closeIsIdempotent() {
        ReconnectSupervisor supervisor = supervisor(() -> {}, () -> {});
        supervisor.close();
        supervisor.close();
    }

    @Test
    @DisplayName("arming after close does nothing")
    void armingAfterCloseIsIgnored() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        ReconnectSupervisor supervisor = supervisor(
                () -> {
                    attempts.incrementAndGet();
                    throw new ConnectionException("still down");
                },
                () -> {});
        supervisor.close();

        supervisor.arm(new ConnectionException("dropped"));

        Thread.sleep(120);
        assertEquals(0, attempts.get());
    }

    @Test
    @DisplayName("backoff doubles from two seconds to a sixty-second ceiling, then holds")
    void backoffDoublesToTheCeiling() {
        ReconnectSupervisor supervisor = new ReconnectSupervisor(() -> {}, () -> {});
        try (supervisor) {
            // Attempt two is the first retry, so the ceilings run 2, 4, 8, …
            assertCeiling(supervisor, 2, 2_000);
            assertCeiling(supervisor, 3, 4_000);
            assertCeiling(supervisor, 4, 8_000);
            assertCeiling(supervisor, 5, 16_000);
            assertCeiling(supervisor, 6, 32_000);
            assertCeiling(supervisor, 7, 60_000);
            assertCeiling(supervisor, 40, 60_000);
            // Far enough out that an unguarded shift would have overflowed.
            assertCeiling(supervisor, 1_000, 60_000);
        }
    }

    /** Draws repeatedly, because the delay is jittered rather than fixed. */
    private static void assertCeiling(ReconnectSupervisor supervisor, int attempt, long ceilingMillis) {
        for (int draw = 0; draw < 200; draw++) {
            long drawn = supervisor.delayFor(attempt).toMillis();
            assertTrue(
                    drawn >= 0 && drawn <= ceilingMillis,
                    "attempt " + attempt + " drew " + drawn + "ms, outside [0, " + ceilingMillis + "]");
        }
    }
}
