// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.CancellationSignal;
import dev.slsk.CancellationSubscription;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;

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
     * @throws CancellationException if cancellation is requested before or
     *     during the wait
     */
    public static void acquire(Semaphore semaphore, CancellationSignal cancellationSignal) {
        cancellationSignal.throwIfCancellationRequested();
        if (semaphore.tryAcquire()) {
            return;
        }

        Thread waiter = Thread.currentThread();
        try (CancellationSubscription registration = cancellationSignal.register(waiter::interrupt)) {
            semaphore.acquire();
        } catch (InterruptedException interrupted) {
            // Reported by the exception, not by the flag: catching
            // InterruptedException already cleared it, and re-setting it would
            // break the caller's next blocking call for a cancellation it has
            // just been told about.
            throw new CancellationException("The operation was cancelled");
        } finally {
            // A cancellation landing just after the permit was taken leaves the
            // flag set on a thread that goes on to do more work. The permit is
            // held and the caller's own signal check is what ends the operation.
            Thread.interrupted();
        }
    }
}
