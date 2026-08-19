// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.concurrent;

/**
 * A disposable subscription to cancellation.
 */
@FunctionalInterface
public interface CancellationSubscription extends AutoCloseable {
    /**
     * Removes the cancellation listener.
     *
     * <p>Closing a subscription more than once has no effect.
     */
    @Override
    void close();
}
