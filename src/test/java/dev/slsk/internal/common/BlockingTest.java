// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.SoulseekClientException;
import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The failure translation every blocking facet method depends on.
 *
 * <p>It lived on the client as {@code unwrapped}, reachable only by building a
 * client and provoking a network failure through it. It is a pure function of a
 * future, so it can be asserted directly, which is what these do.
 */
class BlockingTest {

    @Test
    void returnsTheResultOfACompletedOperation() {
        assertEquals("value", Blocking.await(CompletableFuture.completedFuture("value")));
    }

    @Test
    @DisplayName("a runtime failure arrives as itself, not wrapped in a CompletionException")
    void unwrapsRuntimeFailures() {
        IllegalStateException cause = new IllegalStateException("boom");
        assertSame(
                cause,
                assertThrows(IllegalStateException.class, () -> Blocking.await(CompletableFuture.failedFuture(cause))));
    }

    @Test
    @DisplayName("a lapsed deadline becomes NoResponseException rather than a checked TimeoutException")
    void mapsTimeoutToNoResponse() {
        TimeoutException cause = new TimeoutException("timed out");
        NoResponseException mapped =
                assertThrows(NoResponseException.class, () -> Blocking.await(CompletableFuture.failedFuture(cause)));
        assertSame(cause, mapped.getCause());
        assertEquals("timed out", mapped.getMessage());
    }

    /**
     * Cancellation stays cancellation.
     *
     * <p>Two shapes reach here and both must. Every operation that goes through
     * the failure mapper arrives already wrapped in a {@link
     * CompletionException}, and unwrapping hands back the caller's own
     * exception. A future failed with a bare {@code CancellationException}
     * instead hits {@code join}'s own behaviour, which raises a fresh one
     * carrying the original as its cause — so the identity is in the cause
     * rather than in the exception, and it is still not turned into something
     * that reads as a fault.
     */
    @Test
    @DisplayName("cancellation is preserved, because a caller asked for it")
    void preservesCancellation() {
        CancellationException cause = new CancellationException("cancelled");

        assertSame(
                cause,
                assertThrows(
                        CancellationException.class,
                        () -> Blocking.await(CompletableFuture.failedFuture(new CompletionException(cause)))));

        assertSame(
                cause,
                assertThrows(CancellationException.class, () -> Blocking.await(CompletableFuture.failedFuture(cause)))
                        .getCause());
    }

    @Test
    @DisplayName("a checked failure with no home in the hierarchy becomes SoulseekClientException")
    void wrapsCheckedFailures() {
        IOException cause = new IOException("disk went away");
        SoulseekClientException mapped = assertThrows(
                SoulseekClientException.class, () -> Blocking.await(CompletableFuture.failedFuture(cause)));
        assertSame(cause, mapped.getCause());
    }

    @Test
    void rethrowsErrorsUntouched() {
        StackOverflowError cause = new StackOverflowError();
        assertSame(
                cause,
                assertThrows(StackOverflowError.class, () -> Blocking.await(CompletableFuture.failedFuture(cause))));
    }

    @Test
    @DisplayName("nested completion wrappers are stripped down to the real cause")
    void unwrapsNestedCompletionExceptions() {
        IllegalArgumentException cause = new IllegalArgumentException("bad");
        Throwable nested = new CompletionException(new CompletionException(cause));
        assertSame(
                cause,
                assertThrows(
                        IllegalArgumentException.class, () -> Blocking.await(CompletableFuture.failedFuture(nested))));
    }
}
