// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.Objects;
import java.util.concurrent.CancellationException;

/**
 * An immutable observation handle for cooperative cancellation.
 */
public final class CancellationToken {
    private static final CancellationToken NONE = new CancellationToken(new CancellationState(false));

    private final CancellationState state;

    CancellationToken(CancellationState state) {
        this.state = state;
    }

    /**
     * Returns a token that can never be cancelled.
     *
     * @return the non-cancellable token
     */
    public static CancellationToken none() {
        return NONE;
    }

    /**
     * Returns whether cancellation has been requested.
     *
     * @return {@code true} after cancellation is requested
     */
    public boolean isCancellationRequested() {
        return state.isCancellationRequested();
    }

    /**
     * Throws when cancellation has already been requested.
     *
     * @throws CancellationException when cancellation was requested
     */
    public void throwIfCancellationRequested() {
        if (isCancellationRequested()) {
            throw new CancellationException("The operation was cancelled");
        }
    }

    /**
     * Registers a listener to run synchronously when cancellation is requested.
     *
     * <p>If cancellation was already requested, the listener runs before this
     * method returns.
     *
     * @param listener the cancellation listener
     * @return a registration that can remove the listener
     */
    public CancellationRegistration register(Runnable listener) {
        return state.register(Objects.requireNonNull(listener, "listener"));
    }
}
