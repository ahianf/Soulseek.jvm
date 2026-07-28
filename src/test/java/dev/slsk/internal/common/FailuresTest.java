// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.UserOfflineException;
import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The rule every collaborator applies to a failed operation.
 *
 * <p>It was reachable only through the client that owned it, so what it did was
 * asserted a dozen times over as a side effect of testing something else.
 */
class FailuresTest {

    private static Throwable failureOf(CompletableFuture<?> operation) {
        return assertThrows(CompletionException.class, operation::join).getCause();
    }

    @Test
    @DisplayName("nested completion wrappers are stripped down to the real cause")
    void unwrapsNestedCompletionExceptions() {
        IllegalArgumentException cause = new IllegalArgumentException("bad");
        assertSame(cause, Failures.unwrap(new CompletionException(new CompletionException(cause))));
    }

    @Test
    @DisplayName("a failure that is not a completion wrapper is returned as it stands")
    void leavesOrdinaryFailuresAlone() {
        IllegalStateException failure = new IllegalStateException("plain");
        assertSame(failure, Failures.unwrap(failure));
    }

    @Test
    void aMissingMessageReadsAsEmptyRatherThanNull() {
        assertEquals("", Failures.message(new IllegalStateException()));
        assertEquals("said something", Failures.message(new IllegalStateException("said something")));
    }

    @Test
    void passesASuccessfulOperationThrough() {
        assertEquals(
                "value",
                Failures.map(CompletableFuture.completedFuture("value"), "prefix: ")
                        .join());
    }

    @Test
    @DisplayName("an ordinary failure is wrapped, prefixed with what was being attempted")
    void wrapsOrdinaryFailures() {
        IOException cause = new IOException("socket closed");
        Throwable actual =
                failureOf(Failures.map(CompletableFuture.failedFuture(cause), "Failed to send a private message: "));

        SoulseekClientException mapped = assertThrows(SoulseekClientException.class, () -> {
            throw actual;
        });
        assertSame(cause, mapped.getCause());
        assertEquals("Failed to send a private message: socket closed", mapped.getMessage());
    }

    @Test
    @DisplayName("cancellation and timeout are decisions, not faults, and are never rewritten")
    void preservesCancellationAndTimeout() {
        CancellationException cancelled = new CancellationException("cancelled");
        assertSame(cancelled, failureOf(Failures.map(CompletableFuture.failedFuture(cancelled), "prefix: ")));

        TimeoutException timeout = new TimeoutException("timed out");
        assertSame(timeout, failureOf(Failures.map(CompletableFuture.failedFuture(timeout), "prefix: ")));
    }

    @Test
    @DisplayName("a named failure type passes through untranslated")
    void preservesTheFailuresACallerAskedToKeep() {
        UserOfflineException offline = new UserOfflineException("alice is offline");
        assertSame(
                offline,
                failureOf(
                        Failures.map(CompletableFuture.failedFuture(offline), "prefix: ", UserOfflineException.class)));

        // and only those: an unnamed type is still wrapped
        IllegalStateException other = new IllegalStateException("something else");
        assertSame(
                other,
                failureOf(Failures.map(CompletableFuture.failedFuture(other), "prefix: ", UserOfflineException.class))
                        .getCause());
    }
}
