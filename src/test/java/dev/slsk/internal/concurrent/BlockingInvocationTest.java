// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.common.Scheduler;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;

/** Atomic-winner and whole-deadline rules at the shared public-call boundary. */
class BlockingInvocationTest {

    @Test
    void aPreexistingInterruptWinsBeforeTheOperationCanChangeAnything() {
        AtomicBoolean called = new AtomicBoolean();
        Thread.currentThread().interrupt();
        try {
            assertThrows(
                    InterruptedException.class,
                    () -> BlockingInvocation.run(signal -> {
                        called.set(true);
                        return null;
                    }));
            assertFalse(called.get());
            assertFalse(Thread.interrupted(), "the interrupt reported as InterruptedException was not consumed");
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void aCommittedResultWinsAndLeavesTheLaterInterruptForEnclosingCode() throws Exception {
        CountDownLatch committed = new CountDownLatch(1);
        AtomicReference<Integer> result = new AtomicReference<>();
        AtomicBoolean interruptStillSet = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread caller = Thread.ofVirtual().start(() -> {
            try {
                result.set(BlockingInvocation.run(signal -> {
                    committed.countDown();
                    LockSupport.park();
                    return 42;
                }));
                interruptStillSet.set(Thread.interrupted());
            } catch (Throwable thrown) {
                failure.set(thrown);
            }
        });

        assertTrue(committed.await(5, TimeUnit.SECONDS));
        caller.interrupt();
        caller.join();

        assertEquals(42, result.get());
        assertTrue(interruptStillSet.get(), "the result winner consumed an interrupt belonging to enclosing code");
        assertEquals(null, failure.get());
    }

    @Test
    void interruptionWinsAndAResultCannotLaterTurnItIntoSuccess() throws Exception {
        CountDownLatch parked = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread caller = Thread.ofVirtual().start(() -> {
            try {
                BlockingInvocation.run(signal -> {
                    parked.countDown();
                    new CountDownLatch(1).await();
                    return 42;
                });
            } catch (Throwable thrown) {
                failure.set(thrown);
            }
        });

        assertTrue(parked.await(5, TimeUnit.SECONDS));
        caller.interrupt();
        caller.join();

        assertInstanceOf(InterruptedException.class, failure.get());
    }

    @Test
    void timeoutWinsEvenWhenAnUncooperativeOperationReturnsLater() throws Exception {
        try (Scheduler scheduler = new Scheduler("deadline-result-race")) {
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread caller = Thread.ofVirtual().start(() -> {
                try {
                    BlockingInvocation.run(scheduler, Duration.ofMillis(50), signal -> {
                        entered.countDown();
                        awaitUninterruptibly(release);
                        return 42;
                    });
                } catch (Throwable thrown) {
                    failure.set(thrown);
                }
            });

            assertTrue(entered.await(5, TimeUnit.SECONDS));
            Thread.sleep(100);
            release.countDown();
            caller.join();
            assertInstanceOf(TimeoutException.class, failure.get());
        }
    }

    @Test
    void aZeroDeadlineExpiresBeforeTheOperationCanChangeAnything() throws Exception {
        try (Scheduler scheduler = new Scheduler("zero-deadline")) {
            AtomicBoolean called = new AtomicBoolean();
            assertThrows(
                    TimeoutException.class,
                    () -> BlockingInvocation.run(scheduler, Duration.ZERO, signal -> {
                        called.set(true);
                        return null;
                    }));
            assertFalse(called.get());
        }
    }

    @Test
    void theDeadlineCoversEveryStageRatherThanRestartingAfterTheFirst() throws Exception {
        try (Scheduler scheduler = new Scheduler("whole-deadline");
                var stages = Executors.newSingleThreadScheduledExecutor()) {
            CountDownLatch firstStage = new CountDownLatch(1);
            stages.schedule(firstStage::countDown, 180, TimeUnit.MILLISECONDS);

            long started = System.nanoTime();
            assertThrows(
                    TimeoutException.class,
                    () -> BlockingInvocation.run(scheduler, Duration.ofMillis(250), signal -> {
                        firstStage.await();
                        CountDownLatch secondStage = new CountDownLatch(1);
                        try (CancellationSubscription ignored = signal.register(secondStage::countDown)) {
                            secondStage.await();
                            signal.throwIfCancellationRequested();
                            return null;
                        }
                    }));
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

            assertTrue(elapsed >= 180, "the deliberately slow first stage did not run");
            assertTrue(elapsed < 375, "the 250 ms deadline was restarted after the 180 ms first stage");
        }
    }

    @Test
    void timeoutCleanupFailureIsSuppressedOntoThePrimaryException() throws Exception {
        try (Scheduler scheduler = new Scheduler("deadline-cleanup-failure")) {
            TimeoutException timeout = assertThrows(
                    TimeoutException.class,
                    () -> BlockingInvocation.run(scheduler, Duration.ofMillis(50), signal -> {
                        CountDownLatch cancelled = new CountDownLatch(1);
                        try (CancellationSubscription release = signal.register(cancelled::countDown);
                                CancellationSubscription failing = signal.register(() -> {
                                    throw new IllegalStateException("cleanup failed");
                                })) {
                            cancelled.await();
                            signal.throwIfCancellationRequested();
                            return null;
                        }
                    }));

            assertEquals(1, timeout.getSuppressed().length);
            assertEquals("cleanup failed", timeout.getSuppressed()[0].getMessage());
        }
    }

    @Test
    void cleanupCannotHoldTheCallerPastTheBudget() throws Exception {
        try (Scheduler scheduler = new Scheduler("bounded-deadline-cleanup")) {
            CountDownLatch releaseCleanup = new CountDownLatch(1);
            long started = System.nanoTime();
            try {
                assertThrows(
                        TimeoutException.class,
                        () -> BlockingInvocation.run(scheduler, Duration.ofMillis(50), signal -> {
                            CountDownLatch cancelled = new CountDownLatch(1);
                            try (CancellationSubscription slow =
                                            signal.register(() -> awaitUninterruptibly(releaseCleanup));
                                    CancellationSubscription release = signal.register(cancelled::countDown)) {
                                cancelled.await();
                                signal.throwIfCancellationRequested();
                                return null;
                            }
                        }));
                long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
                assertTrue(elapsed < 750, "cleanup held the caller for " + elapsed + " ms");
            } finally {
                releaseCleanup.countDown();
            }
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
