// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.SoulseekClientException;
import java.util.concurrent.CancellationException;
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
     * Returns a failure's message, never {@code null}.
     *
     * @param failure the failure
     * @return the message, or the empty string
     */
    public static String message(Throwable failure) {
        return failure.getMessage() == null ? "" : failure.getMessage();
    }

    /**
     * Rethrows a stored failure as itself.
     *
     * <p>For the settle-once cells: a failure that was caught on one thread and
     * is being raised on another arrives statically as {@link Throwable}, and
     * this is the one place that turns it back into what it is. Nothing is
     * wrapped — an unchecked failure, an interruption and a lapsed deadline all
     * come back as themselves, which is why every caller declares the two
     * checked outcomes. A checked exception outside those two cannot legally
     * cross the settle boundary; the closing wrap names it rather than losing
     * it, and seeing that wrap in the wild means a settle site is storing
     * something it should have translated.
     *
     * @param failure the stored failure
     * @return never; the return type exists so a caller can write {@code throw}
     * @throws InterruptedException the stored failure, when it is one
     * @throws TimeoutException the stored failure, when it is one
     */
    public static RuntimeException rethrow(Throwable failure) throws InterruptedException, TimeoutException {
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure instanceof InterruptedException interrupted) {
            throw interrupted;
        }
        if (failure instanceof TimeoutException timeout) {
            throw timeout;
        }
        throw new SoulseekClientException(message(failure), failure);
    }

    /**
     * Rethrows a failed operation's real failure, in this library's terms.
     *
     * <p>The blocking replacement for the deleted {@code map} composed with the
     * await that always followed it. Those were two halves of one rule and
     * lived apart only because one ran inside a future and the other outside
     * it: translate the fault, pass a decision through, and never hand a caller
     * a wrapper from the async layer.
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
            Throwable cause, String prefix, Class<? extends Throwable>... preservedFailures)
            throws InterruptedException {
        // An interrupt is the caller's own signal and is never rewritten:
        // burying it under "the operation failed" is what forced the facade
        // boundary to dig through cause chains to honor its contract.
        if (cause instanceof InterruptedException interrupted) {
            throw interrupted;
        }
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
    public static RuntimeException surface(Throwable cause) {
        if (cause instanceof TimeoutException) {
            throw new NoResponseException(cause.getMessage(), cause);
        }
        throw asUnchecked(cause, message(cause));
    }

    private static RuntimeException asUnchecked(Throwable cause, String message) {
        if (cause instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        throw new SoulseekClientException(message, cause);
    }

    /**
     * An {@link IllegalStateException} that skips its stack-trace capture.
     *
     * <p>For a routine outcome that is reported as an exception because that is
     * the shape of the field carrying it — a connection closed on purpose, a
     * delivery that failed because a peer went away. Such an exception is
     * constructed on the reporting path, so the trace it would capture points
     * at the reporter, not at anything that went wrong; filling it in is pure
     * cost. A JFR baseline measured 91,000 of these traces captured in eleven
     * hours, every one discarded unread.
     *
     * @param message the message
     * @return the exception, with an empty stack trace
     */
    public static IllegalStateException stacklessIllegalState(String message) {
        return new StacklessIllegalStateException(message);
    }

    private static final class StacklessIllegalStateException extends IllegalStateException {
        StacklessIllegalStateException(String message) {
            super(message);
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }
}
