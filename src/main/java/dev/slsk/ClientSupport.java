// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.common.CommonUtils;
import dev.slsk.exceptions.SoulseekClientException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

/**
 * Failure translation and argument checks shared by the client's components.
 *
 * <p>These were private statics on the client. Splitting it apart left several
 * components needing the same three or four helpers, and a shared home beats
 * either copying them or widening {@link ClientContext} with utilities that
 * have nothing to do with the seam.
 */
final class ClientSupport {

    private ClientSupport() {}

    static <T> CompletableFuture<T> mapClientFailure(
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
            throw new CompletionException(new SoulseekClientException(prefix + failureMessage(cause), cause));
        });
    }

    static void requireText(String value, String name) {
        if (CommonUtils.isNullOrWhiteSpace(value)) {
            throw new IllegalArgumentException(name + " must not be null, empty, or whitespace");
        }
    }

    static void requireNonEmpty(String value, String name) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be null or empty");
        }
    }

    static String failureMessage(Throwable failure) {
        return failure.getMessage() == null ? "" : failure.getMessage();
    }

    static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
