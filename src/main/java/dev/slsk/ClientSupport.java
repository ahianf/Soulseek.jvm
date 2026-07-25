// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.common.CommonUtils;
import dev.slsk.common.NetworkExecutor;
import dev.slsk.exceptions.SoulseekClientException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Semaphore;
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
    /**
     * Acquires a permit, completing when one is available.
     *
     * <p>This used to spin {@code tryAcquire(50, MILLISECONDS)} on a virtual
     * thread so that it could notice cancellation between attempts, emulating
     * C#'s natively cancellable {@code SemaphoreSlim.WaitAsync(token)}. A
     * hundred queued transfers meant two thousand pointless wakeups a second.
     *
     * <p>The wait now blocks on a virtual thread, which costs nothing while
     * parked, and cancellation arrives as an interrupt.
     */
    static CompletableFuture<Void> acquirePermit(Semaphore semaphore, CancellationSignal cancellationSignal) {
        try {
            cancellationSignal.throwIfCancellationRequested();
            if (semaphore.tryAcquire()) {
                return CompletableFuture.completedFuture(null);
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }

        CompletableFuture<Void> result = new CompletableFuture<>();
        NetworkExecutor.runAsync(() -> {
            Thread waiter = Thread.currentThread();
            CancellationSubscription registration = cancellationSignal.register(waiter::interrupt);
            boolean acquired = false;
            try {
                semaphore.acquire();
                acquired = true;
                if (!result.complete(null)) {
                    // Someone else already completed the future; do not strand
                    // the permit we just took.
                    semaphore.release();
                }
            } catch (InterruptedException failure) {
                result.completeExceptionally(new CancellationException("The operation was cancelled"));
            } catch (Throwable failure) {
                result.completeExceptionally(failure);
            } finally {
                registration.close();
                // A cancellation racing a successful acquire can leave the
                // interrupt set after the permit is taken. This thread is about
                // to die, but clear it so nothing observes a stray flag.
                if (Thread.interrupted() && acquired && result.isCompletedExceptionally()) {
                    semaphore.release();
                }
            }
        });
        return result;
    }
}
