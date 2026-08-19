// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationController;
import dev.slsk.CancellationSignal;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The permit wait, which is now the caller's own thread rather than a hop.
 */
class PermitsTest {

    @Test
    @DisplayName("an available permit is taken without blocking")
    void takesAnAvailablePermit() throws Exception {
        Semaphore semaphore = new Semaphore(1);
        Permits.acquire(semaphore, CancellationSignal.none());
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    @DisplayName("a signal already cancelled is refused before the semaphore is touched")
    void refusesAnAlreadyCancelledSignal() {
        Semaphore semaphore = new Semaphore(1);
        try (CancellationController controller = new CancellationController()) {
            controller.cancel();
            assertThrows(CancellationException.class, () -> Permits.acquire(semaphore, controller.getSignal()));
        }
        assertEquals(1, semaphore.availablePermits(), "a refused acquisition must not take a permit");
    }

    @Test
    @DisplayName("the wait blocks the caller and ends when a permit is released")
    void blocksUntilAPermitIsAvailable() throws Exception {
        Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        CountDownLatch acquired = new CountDownLatch(1);
        Thread waiter = Thread.ofVirtual().start(() -> {
            try {
                Permits.acquire(semaphore, CancellationSignal.none());
                acquired.countDown();
            } catch (InterruptedException unexpected) {
                throw new AssertionError(unexpected);
            }
        });

        assertFalse(acquired.await(100, TimeUnit.MILLISECONDS), "it should still be waiting");
        semaphore.release();
        assertTrue(acquired.await(5, TimeUnit.SECONDS), "the release should have let it through");
        waiter.join();
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    @DisplayName("cancelling a wait in progress ends it, and takes no permit")
    void cancellingAWaitInProgressEndsIt() throws Exception {
        Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        AtomicReference<Throwable> thrown = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        try (CancellationController controller = new CancellationController()) {
            Thread waiter = Thread.ofVirtual().start(() -> {
                try {
                    Permits.acquire(semaphore, controller.getSignal());
                } catch (Throwable failure) {
                    thrown.set(failure);
                } finally {
                    done.countDown();
                }
            });

            assertFalse(done.await(100, TimeUnit.MILLISECONDS));
            controller.cancel();

            assertTrue(done.await(5, TimeUnit.SECONDS), "cancellation did not end the wait");
            waiter.join();
        }

        assertInstanceOfCancellation(thrown.get());
        semaphore.release();
        assertEquals(1, semaphore.availablePermits(), "a cancelled wait must not have taken a permit");
    }

    @Test
    @DisplayName("a cancelled wait leaves the caller's interrupt flag clear")
    void aCancelledWaitDoesNotLeaveTheFlagSet() throws Exception {
        // The interrupt is how cancellation reaches the parked thread, but it is
        // an implementation detail of the wait. The caller is told by the
        // exception, and a flag left set would break whatever it does next —
        // including the cleanup it runs on the way out.
        Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();

        AtomicReference<Boolean> stillInterrupted = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        try (CancellationController controller = new CancellationController()) {
            Thread waiter = Thread.ofVirtual().start(() -> {
                try {
                    Permits.acquire(semaphore, controller.getSignal());
                } catch (CancellationException expected) {
                    stillInterrupted.set(Thread.currentThread().isInterrupted());
                } catch (InterruptedException unexpected) {
                    throw new AssertionError(unexpected);
                } finally {
                    done.countDown();
                }
            });

            assertFalse(done.await(100, TimeUnit.MILLISECONDS));
            controller.cancel();
            assertTrue(done.await(5, TimeUnit.SECONDS));
            waiter.join();
        }

        assertFalse(stillInterrupted.get(), "the caller was left interrupted");
    }

    @Test
    @DisplayName("a caller interrupt is reported as InterruptedException and consumed")
    void callerInterruptionIsReportedAndConsumed() throws Exception {
        Semaphore semaphore = new Semaphore(0);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        AtomicReference<Boolean> flagInCatch = new AtomicReference<>(true);
        CountDownLatch entered = new CountDownLatch(1);
        Thread waiter = Thread.ofVirtual().start(() -> {
            entered.countDown();
            try {
                Permits.acquire(semaphore, CancellationSignal.none());
            } catch (Throwable failure) {
                thrown.set(failure);
                flagInCatch.set(Thread.currentThread().isInterrupted());
            }
        });
        assertTrue(entered.await(1, TimeUnit.SECONDS));

        waiter.interrupt();
        waiter.join(1_000);

        assertFalse(waiter.isAlive());
        assertTrue(thrown.get() instanceof InterruptedException);
        assertFalse(flagInCatch.get());
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    @DisplayName("a pre-existing interrupt is consumed before an available permit is touched")
    void preExistingInterruptWinsAtEntry() throws Exception {
        Semaphore semaphore = new Semaphore(1);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        AtomicReference<Boolean> flagInCatch = new AtomicReference<>(true);
        Thread waiter = Thread.ofVirtual().start(() -> {
            Thread.currentThread().interrupt();
            try {
                Permits.acquire(semaphore, CancellationSignal.none());
            } catch (Throwable failure) {
                thrown.set(failure);
                flagInCatch.set(Thread.currentThread().isInterrupted());
            }
        });
        waiter.join(1_000);

        assertTrue(thrown.get() instanceof InterruptedException);
        assertFalse(flagInCatch.get());
        assertEquals(1, semaphore.availablePermits());
    }

    private static void assertInstanceOfCancellation(Throwable failure) {
        assertTrue(
                failure instanceof CancellationException, () -> "expected a CancellationException but got " + failure);
    }
}
