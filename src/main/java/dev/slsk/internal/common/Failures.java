// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.exceptions.SoulseekClientException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

/**
 * Failure translation for operations that talk to the network.
 *
 * <p>These were private statics on the client, then statics on a {@code
 * ClientSupport} named after it. There is no client any more; what they
 * actually are is one rule applied everywhere an operation can fail — say what
 * went wrong, in terms of this library's exception hierarchy, without burying
 * the cause under the async layer's wrappers.
 *
 * <p>Two failures are never rewritten. A cancellation is what a caller asked
 * for, and a timeout is a deadline the caller set; wrapping either in "the
 * operation failed" would lose the distinction between a fault and a decision.
 */
public final class Failures {

    private Failures() {}

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

    /**
     * Returns a failure's message, never {@code null}.
     *
     * @param failure the failure
     * @return the message, or the empty string
     */
    public static String message(Throwable failure) {
        return failure.getMessage() == null ? "" : failure.getMessage();
    }

    /**
     * Wraps an operation's ordinary failures in a {@link
     * SoulseekClientException} with a prefix saying what was being attempted.
     *
     * @param operation the operation to translate
     * @param prefix prefixes any wrapped failure
     * @param preservedFailures failure types to pass through untranslated
     * @param <T> the result type
     * @return the operation, with its failures translated
     */
    @SafeVarargs
    public static <T> CompletableFuture<T> map(
            CompletableFuture<T> operation, String prefix, Class<? extends Throwable>... preservedFailures) {
        return operation.handle((result, failure) -> {
            if (failure == null) {
                return result;
            }
            Throwable cause = unwrap(failure);
            if (cause instanceof CancellationException || cause instanceof TimeoutException) {
                throw new CompletionException(cause);
            }
            for (Class<? extends Throwable> preserved : preservedFailures) {
                if (preserved.isInstance(cause)) {
                    throw new CompletionException(cause);
                }
            }
            throw new CompletionException(new SoulseekClientException(prefix + message(cause), cause));
        });
    }
}
