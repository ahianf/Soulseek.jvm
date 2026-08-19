// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.concurrent.CancellationController;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;

class LocksTest {

    @Test
    void cancellationEndsAnInterruptibleLockWaitWithoutTakingTheLock() throws Exception {
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        try (CancellationController controller = new CancellationController()) {
            Thread waiter = Thread.ofVirtual().start(() -> {
                try {
                    Locks.acquire(lock, controller.getSignal());
                } catch (Throwable thrown) {
                    failure.set(thrown);
                } finally {
                    done.countDown();
                }
            });

            assertFalse(done.await(100, TimeUnit.MILLISECONDS));
            controller.cancel();
            assertTrue(done.await(5, TimeUnit.SECONDS));
            waiter.join();
        } finally {
            lock.unlock();
        }

        assertInstanceOf(CancellationException.class, failure.get());
        assertTrue(lock.tryLock(), "the cancelled waiter must not own the lock");
        lock.unlock();
    }
}
