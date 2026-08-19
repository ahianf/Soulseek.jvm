// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.UserOfflineException;
import java.io.IOException;
import java.util.concurrent.CancellationException;
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

    @Test
    void aMissingMessageReadsAsEmptyRatherThanNull() {
        assertEquals("", Failures.message(new IllegalStateException()));
        assertEquals("said something", Failures.message(new IllegalStateException("said something")));
    }

    @Test
    @DisplayName("an ordinary failure is wrapped, prefixed with what was being attempted")
    void wrapsOrdinaryFailures() {
        IOException cause = new IOException("socket closed");

        SoulseekClientException mapped = assertThrows(
                SoulseekClientException.class, () -> Failures.raise(cause, "Failed to send a private message: "));

        assertSame(cause, mapped.getCause());
        assertEquals("Failed to send a private message: socket closed", mapped.getMessage());
    }

    @Test
    @DisplayName("cancellation is a decision, not a fault, and is never rewritten")
    void preservesCancellation() {
        CancellationException cancelled = new CancellationException("cancelled");
        assertSame(cancelled, assertThrows(CancellationException.class, () -> Failures.raise(cancelled, "prefix: ")));
    }

    @Test
    @DisplayName("a deadline keeps its identity in the cause, under the name the hierarchy has for it")
    void preservesTimeoutUnderItsUncheckedName() {
        TimeoutException timeout = new TimeoutException("timed out");
        assertSame(
                timeout,
                assertThrows(NoResponseException.class, () -> Failures.raise(timeout, "prefix: "))
                        .getCause());
    }

    @Test
    @DisplayName("a named failure type passes through untranslated")
    void preservesTheFailuresACallerAskedToKeep() {
        UserOfflineException offline = new UserOfflineException("alice is offline");
        assertSame(
                offline,
                assertThrows(
                        UserOfflineException.class,
                        () -> Failures.raise(offline, "prefix: ", UserOfflineException.class)));

        // and only those: an unnamed type is still wrapped
        IllegalStateException other = new IllegalStateException("something else");
        assertSame(
                other,
                assertThrows(
                                SoulseekClientException.class,
                                () -> Failures.raise(other, "prefix: ", UserOfflineException.class))
                        .getCause());
    }
}
