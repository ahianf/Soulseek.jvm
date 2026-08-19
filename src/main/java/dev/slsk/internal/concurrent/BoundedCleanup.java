// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Runs invocation-cancellation cleanup without allowing cancellation to hang. */
public final class BoundedCleanup {
    /** Maximum time the cancelling caller helps teardown before a reaper owns it. */
    public static final long BUDGET_MILLIS = 250;

    private BoundedCleanup() {}

    /**
     * Runs cleanup on a library-owned worker and briefly waits for it.
     *
     * <p>The worker remains owned by the shared network executor if the budget
     * expires. A second interrupt ends the wait immediately. Cleanup failures
     * are attached to, and never replace, the primary interruption.
     *
     * @param cleanup teardown for the cancelled invocation
     * @param primary the interruption the caller will observe
     */
    public static void afterInterruption(Runnable cleanup, InterruptedException primary) {
        run(cleanup, primary);
    }

    /**
     * Runs deadline-expiry cleanup under the same bound as interruption.
     *
     * @param cleanup teardown for the expired invocation
     * @param primary the timeout the caller will observe
     */
    public static void afterTimeout(Runnable cleanup, java.util.concurrent.TimeoutException primary) {
        run(cleanup, primary);
    }

    private static void run(Runnable cleanup, Throwable primary) {
        CountDownLatch finished = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread.ofVirtual().name("soulseek-cleanup").start(() -> {
            try {
                cleanup.run();
            } catch (Throwable thrown) {
                failure.set(thrown);
            } finally {
                finished.countDown();
            }
        });

        try {
            finished.await(BUDGET_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException secondInterrupt) {
            return;
        }
        Throwable cleanupFailure = failure.get();
        if (cleanupFailure != null) {
            primary.addSuppressed(cleanupFailure);
        }
    }
}
