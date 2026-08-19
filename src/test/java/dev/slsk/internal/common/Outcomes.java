// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import java.util.concurrent.CompletableFuture;

/**
 * Turns a test's configured outcome into what a blocking call would raise.
 *
 * <p>Connection probes are written as {@code CompletableFuture} fields — a
 * completed one for success, a failed one for a particular failure, a pending
 * one to park the caller — and that is a good way to say what a test wants.
 * What it is not is a good way to <em>deliver</em> it: {@code join()} does not
 * rethrow a {@link java.util.concurrent.CancellationException}, it throws a new
 * one, so a probe that joins loses the very identity the test asserts on.
 *
 * <p>This delivers the configured failure itself, unwrapped, so a probe
 * raises exactly what a real connection raises.
 */
public final class Outcomes {

    private Outcomes() {}

    /**
     * Waits for an operation the internals have not finished converting.
     *
     * <p>What the deleted {@code Blocking.await} did, kept for the handful of
     * tests that still exercise a future-returning method — the dead
     * {@code enqueue*} overloads Phase 4 removes. Nothing in {@code src/main}
     * needs it.
     *
     * @param operation the operation to wait for
     * @param <T> the result type
     * @return the operation's result
     */
    public static <T> T await(CompletableFuture<T> operation) {
        try {
            return operation.join();
        } catch (Throwable failure) {
            Throwable cause = failure;
            while (cause instanceof java.util.concurrent.CompletionException && cause.getCause() != null) {
                cause = cause.getCause();
            }
            if (cause instanceof java.util.concurrent.TimeoutException) {
                throw new dev.slsk.exceptions.NoResponseException(cause.getMessage(), cause);
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new dev.slsk.exceptions.SoulseekClientException(cause.getMessage(), cause);
        }
    }

    /**
     * Waits for an outcome and raises its failure as the transport would.
     *
     * @param outcome the configured outcome; a pending one parks the caller
     * @param <T> the result type
     * @return the outcome's value
     */
    public static <T> T raise(CompletableFuture<T> outcome)
            throws InterruptedException, java.util.concurrent.TimeoutException {
        // handle() sees the failure as it was recorded, before join() gets the
        // chance to substitute its own.
        Throwable failure = outcome.handle((value, error) -> error).join();
        if (failure != null) {
            throw Failures.rethrow(failure);
        }
        return outcome.getNow(null);
    }
}
