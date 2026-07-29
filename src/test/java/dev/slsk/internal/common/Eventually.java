// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Waits for something a handler dispatched rather than did.
 *
 * <p>A message handler answers a ping, forwards a search, acknowledges a private
 * message and connects to a peer on threads of their own, because none of it is
 * something the next protocol message should wait behind. The handler used to
 * return a future covering that work, and a test could join it; the future is
 * gone and the dispatch is not, so a test that asserts on the effect has to wait
 * for it.
 *
 * <p>Bounded and then asserted, so a wait that never comes true fails as the
 * condition it was rather than as a timeout.
 */
public final class Eventually {

    private static final long LIMIT_SECONDS = 5;

    private Eventually() {}

    /**
     * Waits for a condition to become true, for up to five seconds.
     *
     * @param condition what to wait for
     * @return whether it came true
     */
    public static boolean holds(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(LIMIT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            Thread.onSpinWait();
        }
        return true;
    }
}
