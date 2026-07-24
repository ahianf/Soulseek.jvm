// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

/**
 * Requests cooperative cancellation through an associated token.
 */
public final class CancellationTokenSource implements AutoCloseable {
    private final CancellationState state = new CancellationState(true);
    private final CancellationToken token = new CancellationToken(state);

    /**
     * Returns the immutable token associated with this source.
     *
     * @return the cancellation token
     */
    public CancellationToken getToken() {
        return token;
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
     * <p>Closing the source more than once has no effect.
     */
    @Override
    public void close() {
        state.close();
    }
}
