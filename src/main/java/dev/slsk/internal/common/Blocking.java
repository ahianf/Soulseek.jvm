// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.SoulseekClientException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

/**
 * The boundary between the future-shaped internals and the blocking surface.
 *
 * <p>Every facet method that reaches the network ends here: it takes the
 * operation the collaborator returned, waits for it, and presents the failure
 * the way a blocking API should. This was a method on the client, which meant
 * the client had to exist for anything to be blocking. It does not have to be
 * anywhere in particular — it is a pure function of a future — so it lives
 * where the facets can reach it without going through a god object.
 *
 * <p>Internally the library still uses {@link CompletableFuture} throughout, by
 * design: removing it is a separate goal. This is the one place that fact is
 * allowed to be visible, and it stops here.
 */
public final class Blocking {

    private Blocking() {}

    /**
     * Waits for an operation and rethrows its real failure.
     *
     * <p>{@code join()} wraps everything in {@link CompletionException}, which
     * is an artifact of the async layer and has no business reaching a caller of
     * a blocking method. This unwraps it and rethrows the cause.
     *
     * <p>A lapsed deadline arrives as the checked {@link TimeoutException}.
     * Declaring that on every operation that talks to the server would put a
     * checked exception on most of the public surface, which is the ceremony
     * this API exists to remove; the rest of the hierarchy is already unchecked.
     * It is therefore mapped to {@link NoResponseException}, which already means
     * "an expected response was not received" and is the semantically correct
     * member of the existing hierarchy.
     *
     * @param operation the operation to wait for
     * @param <T> the result type
     * @return the operation's result
     */
    public static <T> T await(CompletableFuture<T> operation) {
        try {
            return operation.join();
        } catch (Throwable failure) {
            Throwable cause = unwrap(failure);
            if (cause instanceof TimeoutException) {
                throw new NoResponseException(cause.getMessage(), cause);
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new SoulseekClientException(cause.getMessage(), cause);
        }
    }

    /**
     * Strips the {@link CompletionException} wrappers off a failure.
     *
     * @param failure the failure to unwrap
     * @return the innermost cause that is not a completion wrapper
     */
    public static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
