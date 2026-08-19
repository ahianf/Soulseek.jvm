// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.concurrent;

/**
 * Requests cooperative cancellation through an associated signal.
 */
public final class CancellationController implements AutoCloseable {
    private final CancellationState state = new CancellationState(true);
    private final CancellationSignal signal = new CancellationSignal(state);

    /**
     * Returns the immutable signal associated with this controller.
     *
     * @return the cancellation signal
     */
    public CancellationSignal getSignal() {
        return signal;
    }

    /**
     * Requests cancellation and synchronously invokes registered listeners.
     *
     * <p>Cancellation is idempotent. Listeners run in reverse registration
     * order, matching the source platform.
     */
    public void cancel() {
        state.cancel();
    }

    /**
     * Releases all listener registrations without requesting cancellation.
     *
     * <p>Closing the controller more than once has no effect.
     */
    @Override
    public void close() {
        state.close();
    }
}
