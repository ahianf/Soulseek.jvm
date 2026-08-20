// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.internal.concurrent.CancellationController;
import dev.slsk.internal.concurrent.CancellationSignal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class WaiterTest {
    private static final Duration LONG_TIMEOUT = Duration.ofSeconds(30);

    @Test
    @DisplayName("Constructors retain default timeouts")
    void constructorsRetainDefaultTimeouts() {
        try (DefaultWaiter defaultWaiter = new DefaultWaiter();
                DefaultWaiter customWaiter = new DefaultWaiter(Duration.ofMillis(42))) {
            assertEquals(DefaultWaiter.DEFAULT_TIMEOUT, defaultWaiter.getDefaultTimeout());
            assertEquals(Duration.ofMillis(42), customWaiter.getDefaultTimeout());
        }
    }

    @Test
    @DisplayName("Complete dequeues and completes the oldest wait")
    void completeDequeuesAndCompletesOldestWait() throws Exception {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey.Named("login");
            Wait<String> first = waiter.register(key, String.class, waiter.getDefaultTimeout(), null);
            Wait<String> second = waiter.register(key, String.class, waiter.getDefaultTimeout(), null);

            waiter.complete(key, "first");

            assertEquals("first", first.await());
            assertEquals(1, waiter.getWaitCount(key));
            assertTrue(waiter.hasWait(key));

            waiter.complete(key, "second");

            assertEquals("second", second.await());
            assertFalse(waiter.hasWait(key));
            assertEquals(0, waiter.getKeyCount());
        }
    }

    @Test
    @DisplayName("Non-generic complete returns null")
    void nonGenericCompleteReturnsNull() throws Exception {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey.Named("room-list");
            Wait<Void> wait = waiter.register(key, waiter.getDefaultTimeout(), null);

            waiter.complete(key);

            assertNull(wait.await());
        }
    }

    @Test
    @DisplayName("Disposition of a missing wait does not throw")
    void missingWaitDoesNotThrow() {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey.Named("missing");

            assertDoesNotThrow(() -> waiter.complete(key));
            assertDoesNotThrow(() -> waiter.cancel(key));
            assertDoesNotThrow(() -> waiter.timeout(key));
            assertDoesNotThrow(() -> waiter.fail(key, new RuntimeException("failure")));
        }
    }

    @Test
    @DisplayName("Cancel dequeues and cancels the oldest wait")
    void cancelDequeuesAndCancelsOldestWait() {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey.Named("login");
            Wait<Void> wait = waiter.register(key, waiter.getDefaultTimeout(), null);

            waiter.cancel(key);

            assertThrows(CancellationException.class, wait::await);
            assertFalse(waiter.hasWait(key));
        }
    }

    @Test
    @DisplayName("Cancelling by key takes exactly one wait, oldest first")
    void cancelTakesExactlyOneWaitOldestFirst() throws Exception {
        // A caller can no longer cancel its own wait — a handle is not a
        // future — so cancellation is by key, and the queue behind that key has
        // to survive it.
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey.Named("login");
            Wait<String> first = waiter.register(key, String.class, LONG_TIMEOUT, null);
            Wait<String> second = waiter.register(key, String.class, LONG_TIMEOUT, null);

            waiter.cancel(key);

            assertThrows(CancellationException.class, first::await);
            assertEquals(1, waiter.getWaitCount(key));
            waiter.complete(key, "result");
            assertEquals("result", second.await());
        }
    }

    @Test
    @DisplayName("Manual timeout dequeues with TimeoutException")
    void manualTimeoutDequeuesWithTimeoutException() {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey.Named("login");
            Wait<Void> wait = waiter.register(key, waiter.getDefaultTimeout(), null);

            waiter.timeout(key);

            assertThrows(TimeoutException.class, wait::await);
            assertFalse(waiter.hasWait(key));
        }
    }

    @Test
    @DisplayName("Automatic timeout dequeues only its oldest wait")
    void automaticTimeoutDequeuesOnlyOldestWait() {
        try (DefaultWaiter waiter = new DefaultWaiter(Duration.ZERO)) {
            WaitKey key = new WaitKey.Named("login");
            Wait<String> first = waiter.register(key, String.class, waiter.getDefaultTimeout(), null);
            waiter.register(key, String.class, LONG_TIMEOUT, null);

            assertThrows(TimeoutException.class, first::await);
            assertEquals(1, waiter.getWaitCount(key));
        }
    }

    @Test
    @DisplayName("Caller cancellation dequeues a wait")
    void callerCancellationDequeuesWait() {
        try (DefaultWaiter waiter = new DefaultWaiter();
                CancellationController source = new CancellationController()) {
            WaitKey key = new WaitKey.Named("login");
            Wait<String> wait = waiter.register(key, String.class, LONG_TIMEOUT, source.getSignal());

            source.cancel();

            assertThrows(CancellationException.class, wait::await);
            assertFalse(waiter.hasWait(key));
        }
    }

    @Test
    @DisplayName("Pre-cancelled token is handled after enqueue")
    void preCancelledTokenIsHandledAfterEnqueue() {
        try (DefaultWaiter waiter = new DefaultWaiter();
                CancellationController source = new CancellationController()) {
            source.cancel();
            WaitKey key = new WaitKey.Named("login");

            Wait<String> wait = waiter.register(key, String.class, LONG_TIMEOUT, source.getSignal());

            assertThrows(CancellationException.class, wait::await);
            assertFalse(waiter.hasWait(key));
        }
    }

    /**
     * Deliberately not the C# behaviour. The source's cancel action dequeues
     * the key's oldest wait, so with two callers under one key — CHECK_
     * PRIVILEGES, PING, a shared place-in-queue key — caller B's cancellation
     * settled caller A's wait: A saw a spurious CancellationException, and B
     * stayed waiting to be handed A's response. The signal belongs to one
     * wait, and it is that wait it cancels. Nothing on the wire changes.
     */
    @Test
    @DisplayName("A caller's cancellation settles its own wait, not the oldest under the key")
    void cancellationTargetsTheWaitWhoseSignalFired() throws Exception {
        try (DefaultWaiter waiter = new DefaultWaiter();
                CancellationController secondSource = new CancellationController()) {
            WaitKey key = new WaitKey.Named("login");
            Wait<String> first = waiter.register(key, String.class, LONG_TIMEOUT, null);
            Wait<String> second = waiter.register(key, String.class, LONG_TIMEOUT, secondSource.getSignal());

            secondSource.cancel();

            assertThrows(CancellationException.class, second::await);
            assertEquals(1, waiter.getWaitCount(key), "the uncancelled wait is still registered");

            // And it is still the head: the next response reaches it.
            waiter.complete(key, "answer");
            assertEquals("answer", first.await());
        }
    }

    @Test
    @DisplayName("Fail preserves the supplied exception")
    void failPreservesSuppliedException() {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey.Named("login");
            Wait<String> wait = waiter.register(key, String.class, waiter.getDefaultTimeout(), null);
            RuntimeException failure = new RuntimeException("failure");

            waiter.fail(key, failure);

            RuntimeException thrown = assertThrows(RuntimeException.class, wait::await);
            assertSame(failure, thrown);
        }
    }

    @Test
    @DisplayName("Type mismatch throws SoulseekClientException")
    void typeMismatchThrowsSoulseekClientException() {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey.Named("login");
            waiter.register(key, String.class, waiter.getDefaultTimeout(), null);

            SoulseekClientException exception =
                    assertThrows(SoulseekClientException.class, () -> waiter.complete(key, 42));

            assertTrue(exception.getMessage().contains("mismatch"));
            assertFalse(waiter.hasWait(key));
        }
    }

    @Test
    @DisplayName("registerIndefinitely never times out and parks no timer")
    void registerIndefinitelySchedulesNothing() {
        try (Scheduler scheduler = new Scheduler("waiter-test");
                DefaultWaiter waiter = new DefaultWaiter(Duration.ofSeconds(5), scheduler)) {
            WaitKey key = new WaitKey.Named("transfer");
            Wait<String> wait = waiter.registerIndefinitely(key, String.class, null);

            assertFalse(settles(wait));
            assertEquals(1, waiter.getWaitCount(key));
            assertEquals(
                    0, scheduler.pendingTasksForTest(), "an indefinite wait parked a real timer in the delay queue");
        }
    }

    @Test
    @DisplayName("Null timeout does not schedule expiration")
    void nullTimeoutDoesNotScheduleExpiration() {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey.Named("transfer");
            Wait<String> wait = waiter.register(key, String.class, null, null);

            assertFalse(settles(wait));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> waiter.register(key, String.class, Duration.ofMillis(-2), null));
        }
    }

    @Test
    @DisplayName("CancelAll cancels duplicate-key waits")
    void cancelAllCancelsDuplicateKeyWaits() {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey.Named("login");
            Wait<String> first = waiter.register(key, String.class, LONG_TIMEOUT, null);
            Wait<String> second = waiter.register(key, String.class, LONG_TIMEOUT, null);

            waiter.cancelAll();

            assertThrows(CancellationException.class, first::await);
            assertThrows(CancellationException.class, second::await);
            assertEquals(0, waiter.getKeyCount());
        }
    }

    @Test
    @DisplayName("Close is idempotent, releases waits, and rejects new waits")
    void closeReleasesWaitsAndRejectsNewWaits() {
        DefaultWaiter waiter = new DefaultWaiter();
        Wait<String> pending = waiter.register(new WaitKey.Named("login"), String.class, LONG_TIMEOUT, null);

        waiter.close();
        waiter.close();

        assertThrows(CancellationException.class, pending::await);
        assertThrows(
                IllegalStateException.class,
                () -> waiter.register(new WaitKey.Named("new"), String.class, waiter.getDefaultTimeout(), null)
                        .await());
    }

    @Test
    @DisplayName("PendingWait close works before and after registration")
    void pendingWaitCloseWorksBeforeAndAfterRegistration() {
        try (Scheduler scheduler = new Scheduler("waiter-test-timer")) {
            DefaultWaiter.PendingWait<String> unregistered =
                    new DefaultWaiter.PendingWait<>(String.class, LONG_TIMEOUT, CancellationSignal.none());
            unregistered.actions(() -> {}, () -> {}, () -> {});
            DefaultWaiter.PendingWait<String> registered =
                    new DefaultWaiter.PendingWait<>(String.class, LONG_TIMEOUT, CancellationSignal.none());
            registered.actions(() -> {}, () -> {}, () -> {});

            assertDoesNotThrow(unregistered::close);
            registered.register(scheduler);
            assertDoesNotThrow(registered::close);
        }
    }

    @Test
    @DisplayName("Interrupting a parked wait cancels that wait and consumes the interrupt")
    void interruptionCancelsOnlyTheParkedWait() throws Exception {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey.Named("interrupt");
            Wait<String> wait = waiter.registerIndefinitely(key, String.class, null);
            AtomicReference<Throwable> observed = new AtomicReference<>();
            AtomicBoolean flagInCatch = new AtomicBoolean(true);
            CountDownLatch entered = new CountDownLatch(1);
            Thread thread = Thread.ofVirtual().start(() -> {
                entered.countDown();
                try {
                    wait.await();
                } catch (Throwable failure) {
                    observed.set(failure);
                    flagInCatch.set(Thread.currentThread().isInterrupted());
                }
            });
            assertTrue(entered.await(1, TimeUnit.SECONDS));

            thread.interrupt();
            thread.join(1_000);

            assertFalse(thread.isAlive());
            assertTrue(observed.get() instanceof InterruptedException);
            assertFalse(flagInCatch.get(), "the observed interrupt must be consumed");
            assertFalse(waiter.hasWait(key));
        }
    }

    @Test
    @DisplayName("A committed result wins a later interrupt and leaves its flag set")
    void committedResultWinsAndPreservesLaterInterrupt() throws Exception {
        DefaultWaiter.PendingWait<String> wait =
                new DefaultWaiter.PendingWait<>(String.class, LONG_TIMEOUT, CancellationSignal.none());
        wait.actions(() -> {}, () -> {}, () -> {});
        AtomicReference<String> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean flagAtReturn = new AtomicBoolean();
        CountDownLatch entered = new CountDownLatch(1);
        Thread thread = Thread.ofVirtual().start(() -> {
            entered.countDown();
            try {
                result.set(wait.await());
                flagAtReturn.set(Thread.currentThread().isInterrupted());
            } catch (Throwable thrown) {
                failure.set(thrown);
            }
        });
        assertTrue(entered.await(1, TimeUnit.SECONDS));

        assertTrue(wait.trySettle("answer", null));
        thread.interrupt();
        wait.publish();
        thread.join(1_000);

        assertFalse(thread.isAlive());
        assertNull(failure.get());
        assertEquals("answer", result.get());
        assertTrue(flagAtReturn.get());
    }

    @Test
    @DisplayName("An interrupt that commits first cannot be overwritten by a later result")
    void interruptionWinnerDiscardsLaterResult() throws Exception {
        DefaultWaiter.PendingWait<String> wait =
                new DefaultWaiter.PendingWait<>(String.class, LONG_TIMEOUT, CancellationSignal.none());
        CountDownLatch cleanupEntered = new CountDownLatch(1);
        CountDownLatch releaseCleanup = new CountDownLatch(1);
        wait.actions(() -> {}, () -> {}, () -> {
            cleanupEntered.countDown();
            try {
                releaseCleanup.await();
            } catch (InterruptedException impossible) {
                throw new AssertionError(impossible);
            }
        });
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread thread = Thread.ofVirtual().start(() -> {
            try {
                wait.await();
            } catch (Throwable failure) {
                observed.set(failure);
            }
        });

        thread.interrupt();
        assertTrue(cleanupEntered.await(1, TimeUnit.SECONDS));
        wait.settle("too late", null);
        thread.join(1_000);
        releaseCleanup.countDown();

        assertFalse(thread.isAlive(), "bounded cleanup must release the interrupted caller");
        assertTrue(observed.get() instanceof InterruptedException);
    }

    @Test
    @DisplayName("Concurrent enqueue and complete does not discard waits")
    void concurrentEnqueueAndCompleteDoesNotDiscardWaits() throws InterruptedException {
        int count = 500;
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey.Named("concurrent");
            List<Wait<Integer>> waits = Collections.synchronizedList(new ArrayList<>());

            Thread producer = Thread.ofPlatform().start(() -> {
                for (int index = 0; index < count; index++) {
                    waits.add(waiter.registerIndefinitely(key, Integer.class, null));
                }
            });
            Thread consumer = Thread.ofPlatform().start(() -> {
                for (int index = 0; index < count; index++) {
                    while (!waiter.hasWait(key)) {
                        Thread.onSpinWait();
                    }
                    waiter.complete(key, index);
                }
            });

            producer.join();
            consumer.join();

            assertEquals(count, waits.size());
            for (Wait<Integer> wait : waits) {
                assertDoesNotThrow(wait::await);
            }
            assertEquals(0, waiter.getKeyCount());
        }
    }

    /**
     * Whether a wait settles within a short window.
     *
     * <p>A handle has no {@code isDone}, which is the point: the only way to
     * observe one is to wait on it. Asserting the negative is safe on a slow
     * machine — a machine too busy to run this in time is also too busy to
     * settle a wait nothing has answered.
     */
    private static boolean settles(Wait<?> wait) {
        CountDownLatch settled = new CountDownLatch(1);
        AtomicReference<Thread> waiter = new AtomicReference<>();
        waiter.set(Thread.ofVirtual().start(() -> {
            try {
                wait.await();
            } catch (Throwable ignored) {
                // Settling exceptionally is still settling.
            }
            settled.countDown();
        }));
        try {
            return settled.await(50, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }
}
