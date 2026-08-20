// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.internal.concurrent.CancellationInterrupts;
import dev.slsk.internal.concurrent.CancellationSignal;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;

/**
 * Acquiring a semaphore permit without burning a core waiting for it.
 *
 * <p>This used to spin {@code tryAcquire(50, MILLISECONDS)} on a virtual thread
 * so that it could notice cancellation between attempts. A hundred queued
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

        CancellationInterrupts.interruptOnCancel(
                cancellationSignal,
                () -> {
                    semaphore.acquire();
                    return semaphore;
                },
                Semaphore::release);
    }
}
