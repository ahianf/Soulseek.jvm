// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.CancellationSignal;
import dev.slsk.CancellationSubscription;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

/**
 * Acquiring a semaphore permit without burning a core waiting for it.
 *
 * <p>This used to spin {@code tryAcquire(50, MILLISECONDS)} on a virtual thread
 * so that it could notice cancellation between attempts, emulating C#'s
 * natively cancellable {@code SemaphoreSlim.WaitAsync(token)}. A hundred queued
 * transfers meant two thousand pointless wakeups a second.
 *
 * <p>The wait now blocks on a virtual thread, which costs nothing while parked,
 * and cancellation arrives as an interrupt.
 */
public final class Permits {

    private Permits() {}

    /**
     * Acquires a permit, completing when one is available.
     *
     * @param semaphore the semaphore to acquire from
     * @param cancellationSignal the cancellation signal
     * @return an operation completing once the permit is held
     */
    public static CompletableFuture<Void> acquire(Semaphore semaphore, CancellationSignal cancellationSignal) {
        try {
            cancellationSignal.throwIfCancellationRequested();
            if (semaphore.tryAcquire()) {
                return CompletableFuture.completedFuture(null);
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }

        CompletableFuture<Void> result = new CompletableFuture<>();
        NetworkExecutor.runAsync(() -> {
            Thread waiter = Thread.currentThread();
            CancellationSubscription registration = cancellationSignal.register(waiter::interrupt);
            boolean acquired = false;
            try {
                semaphore.acquire();
                acquired = true;
                if (!result.complete(null)) {
                    // Someone else already completed the future; do not strand
                    // the permit we just took.
                    semaphore.release();
                }
            } catch (InterruptedException failure) {
                result.completeExceptionally(new CancellationException("The operation was cancelled"));
            } catch (Throwable failure) {
                result.completeExceptionally(failure);
            } finally {
                registration.close();
                // A cancellation racing a successful acquire can leave the
                // interrupt set after the permit is taken. This thread is about
                // to die, but clear it so nothing observes a stray flag.
                if (Thread.interrupted() && acquired && result.isCompletedExceptionally()) {
                    semaphore.release();
                }
            }
        });
        return result;
    }
}
