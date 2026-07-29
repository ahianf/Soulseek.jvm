// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.exceptions.NoResponseException;
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
     * Rethrows a failure the way {@link CompletableFuture#join()} presented it.
     *
     * <p>Blocking internals raise the failures that used to arrive through a
     * future, and every call site in this library was written against what
     * {@code join()} produced: a {@link CancellationException} raw, a
     * {@link CompletionException} passed straight through, and anything else
     * wrapped in one. Keeping that shape is what let the future come out from
     * under several hundred call sites without any of them changing how they
     * read a failure — {@link #unwrap(Throwable)} and an {@code instanceof}
     * still mean what they meant.
     *
     * <p>Declared to return so a caller can write {@code throw propagate(x)}
     * and the compiler can see the path ends there. It never returns.
     *
     * @param failure the failure to rethrow
     * @return never; the return type exists for definite assignment
     */
    public static RuntimeException propagate(Throwable failure) {
        if (failure instanceof CancellationException cancellation) {
            throw cancellation;
        }
        if (failure instanceof CompletionException completion) {
            throw completion;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new CompletionException(failure);
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
     * Rethrows a failed operation's real failure, in this library's terms.
     *
     * <p>The blocking replacement for {@link #map} composed with the await that
     * always followed it. Those were two halves of one rule and lived apart
     * only because one ran inside a future and the other outside it: translate
     * the fault, pass a decision through, and never hand a caller a wrapper
     * from the async layer.
     *
     * <p>A lapsed deadline arrives as the checked {@link TimeoutException}.
     * Declaring that on every operation that talks to the server would put a
     * checked exception on most of the surface, which is the ceremony this API
     * exists to remove; it becomes {@link NoResponseException}, which already
     * means "an expected response was not received".
     *
     * @param failure the failure to translate
     * @param prefix prefixes any wrapped failure
     * @param preservedFailures failure types to rethrow untranslated
     * @return never; the return type exists so a caller can write {@code throw}
     */
    @SafeVarargs
    public static RuntimeException raise(
            Throwable failure, String prefix, Class<? extends Throwable>... preservedFailures) {
        Throwable cause = unwrap(failure);
        // NoResponseException alongside the other two because it *is* the
        // surfaced deadline: an inner layer that has already named a timeout
        // must not have it renamed "the operation failed" by an outer one.
        if (cause instanceof CancellationException
                || cause instanceof TimeoutException
                || cause instanceof NoResponseException) {
            throw surface(cause);
        }
        for (Class<? extends Throwable> preserved : preservedFailures) {
            if (preserved.isInstance(cause)) {
                throw surface(cause);
            }
        }
        throw surface(new SoulseekClientException(prefix + message(cause), cause));
    }

    /**
     * The last translation before a caller sees a failure.
     *
     * <p>Says what went wrong in this library's terms without adding anything:
     * a decision the caller made comes back as itself, and the one fault that
     * cannot come back as itself — the checked {@link TimeoutException} — takes
     * the name the hierarchy already has for it.
     *
     * @param failure the failure to surface
     * @return never; the return type exists so a caller can write {@code throw}
     */
    public static RuntimeException surface(Throwable failure) {
        Throwable cause = unwrap(failure);
        if (cause instanceof TimeoutException) {
            throw new NoResponseException(cause.getMessage(), cause);
        }
        throw rethrow(cause, message(cause));
    }

    private static RuntimeException rethrow(Throwable cause, String message) {
        if (cause instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        throw new SoulseekClientException(message, cause);
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
