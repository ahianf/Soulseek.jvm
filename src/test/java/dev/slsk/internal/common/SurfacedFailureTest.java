// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.RoomJoinForbiddenException;
import dev.slsk.exceptions.SoulseekClientException;
import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The failure translation every blocking method depends on.
 *
 * <p>It lived on the client as {@code unwrapped}, then in {@code Blocking} as a
 * pure function of a future. There are no futures to be a function of any more,
 * so it is {@link Failures#surface}: the last translation before a caller sees
 * a failure. The rules it asserts have not moved.
 */
class SurfacedFailureTest {

    @Test
    @DisplayName("a runtime failure arrives as itself")
    void unwrapsRuntimeFailures() {
        IllegalStateException cause = new IllegalStateException("boom");
        assertSame(cause, assertThrows(IllegalStateException.class, () -> Failures.surface(cause)));
    }

    @Test
    @DisplayName("a lapsed deadline becomes NoResponseException rather than a checked TimeoutException")
    void mapsTimeoutToNoResponse() {
        TimeoutException cause = new TimeoutException("timed out");
        NoResponseException mapped = assertThrows(NoResponseException.class, () -> Failures.surface(cause));
        assertSame(cause, mapped.getCause());
        assertEquals("timed out", mapped.getMessage());
    }

    /**
     * Cancellation stays cancellation: the caller gets back its own exception.
     * That is a change for the better over {@code join()}, which substituted a
     * fresh {@code CancellationException}, putting the identity in the cause
     * rather than in the exception.
     */
    @Test
    @DisplayName("cancellation is preserved, because a caller asked for it")
    void preservesCancellation() {
        CancellationException cause = new CancellationException("cancelled");

        assertSame(cause, assertThrows(CancellationException.class, () -> Failures.surface(cause)));
    }

    @Test
    @DisplayName("raise passes a named failure through and wraps the rest with what was being attempted")
    void raisePreservesNamedFailuresAndPrefixesTheRest() {
        // The two halves of what used to be Failures.map composed with the
        // await that always followed it.
        RoomJoinForbiddenException named = new RoomJoinForbiddenException("private");
        assertSame(
                named,
                assertThrows(
                        RoomJoinForbiddenException.class,
                        () -> Failures.raise(named, "Failed to join chat room x: ", RoomJoinForbiddenException.class)));

        IllegalStateException other = new IllegalStateException("broken");
        SoulseekClientException wrapped = assertThrows(
                SoulseekClientException.class,
                () -> Failures.raise(other, "Failed to join chat room x: ", RoomJoinForbiddenException.class));
        assertEquals("Failed to join chat room x: broken", wrapped.getMessage());
        assertSame(other, wrapped.getCause());
    }

    @Test
    @DisplayName("raise maps a deadline to NoResponseException")
    void raiseMapsTimeoutToNoResponse() {
        TimeoutException cause = new TimeoutException("timed out");
        NoResponseException mapped =
                assertThrows(NoResponseException.class, () -> Failures.raise(cause, "Failed to join chat room x: "));
        assertSame(cause, mapped.getCause());
    }

    @Test
    @DisplayName("raise passes an interrupt through as itself, never wrapped")
    void raisePassesInterruptsThrough() {
        InterruptedException interrupted = new InterruptedException("stopped");
        assertSame(
                interrupted,
                assertThrows(
                        InterruptedException.class, () -> Failures.raise(interrupted, "Failed to join chat room x: ")));
    }

    @Test
    @DisplayName("a checked failure with no home in the hierarchy becomes SoulseekClientException")
    void wrapsCheckedFailures() {
        IOException cause = new IOException("disk went away");
        SoulseekClientException mapped = assertThrows(SoulseekClientException.class, () -> Failures.surface(cause));
        assertSame(cause, mapped.getCause());
    }

    @Test
    void rethrowsErrorsUntouched() {
        StackOverflowError cause = new StackOverflowError();
        assertSame(cause, assertThrows(StackOverflowError.class, () -> Failures.surface(cause)));
    }
}
