// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.concurrent.CancellationSubscription;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Acquiring a semaphore permit without burning a core waiting for it.
 *
 * <p>This used to spin {@code tryAcquire(50, MILLISECONDS)} on a virtual thread
 * so that it could notice cancellation between attempts, emulating C#'s
 * natively cancellable {@code SemaphoreSlim.WaitAsync(token)}. A hundred queued
 * transfers meant two thousand pointless wakeups a second.
 *
 * <p>It then blocked on a virtual thread of its own and completed a future the
 * caller immediately awaited. That cost a thread hop and an allocation per
 * acquisition, and — worse — it created a race that did not otherwise exist:
 * the permit could be taken by the spawned thread after something else had
 * already completed the future, so the code had to detect that and hand the
 * permit back. The whole complete-versus-release dance existed only because of
 * the future.
 *
 * <p>Now the caller's own virtual thread blocks, which is what the caller was
 * doing anyway. A parked virtual thread costs nothing, and cancellation still
 * arrives as an interrupt — which is correct here, unlike on a socket read.
 */
public final class Permits {

    private Permits() {}

    /**
     * Acquires a permit, blocking until one is available.
     *
     * @param semaphore the semaphore to acquire from
     * @param cancellationSignal the cancellation signal
     * @throws CancellationException if the internal signal is cancelled first
     * @throws InterruptedException if the waiting thread is interrupted first
     */
    public static void acquire(Semaphore semaphore, CancellationSignal cancellationSignal) throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException("The permit wait was interrupted before it started");
        }
        cancellationSignal.throwIfCancellationRequested();
        if (semaphore.tryAcquire()) {
            return;
        }

        Thread waiter = Thread.currentThread();
        AtomicReference<Outcome> outcome = new AtomicReference<>(Outcome.WAITING);
        try (CancellationSubscription registration = cancellationSignal.register(() -> {
            if (outcome.compareAndSet(Outcome.WAITING, Outcome.CANCELLED)) {
                waiter.interrupt();
            }
        })) {
            semaphore.acquire();
            if (!outcome.compareAndSet(Outcome.WAITING, Outcome.ACQUIRED)) {
                semaphore.release();
                throw new CancellationException("The operation was cancelled");
            }
        } catch (InterruptedException interrupted) {
            if (outcome.compareAndSet(Outcome.WAITING, Outcome.INTERRUPTED)) {
                throw interrupted;
            }
            if (outcome.get() == Outcome.CANCELLED) {
                throw new CancellationException("The operation was cancelled");
            }
            // Acquisition committed first. Preserve the later interrupt for
            // the caller's enclosing work instead of consuming it here.
            Thread.currentThread().interrupt();
        }
    }

    private enum Outcome {
        WAITING,
        ACQUIRED,
        CANCELLED,
        INTERRUPTED
    }
}
