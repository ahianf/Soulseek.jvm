// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

/**
 * A disposable subscription to cancellation.
 */
@FunctionalInterface
public interface CancellationRegistration extends AutoCloseable {
    /**
     * Removes the cancellation listener.
     *
     * <p>Closing a registration more than once has no effect.
     */
    @Override
    void close();
}
