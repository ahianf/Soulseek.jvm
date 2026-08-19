// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.concurrent;

import dev.slsk.internal.common.Scheduler;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/** The interruption and deadline boundary around one public blocking call. */
public final class BlockingInvocation {

    private BlockingInvocation() {}

    /** Work performed by a blocking facade method using its private signal. */
    @FunctionalInterface
    public interface Operation<T> {
        T run(CancellationSignal signal) throws InterruptedException;
    }

    /** Runs an invocation with no caller deadline. */
    public static <T> T run(Operation<T> operation) throws InterruptedException {
        entryCheck();
        try {
            return execute(null, null, Objects.requireNonNull(operation, "operation"));
        } catch (TimeoutException impossible) {
            throw new AssertionError("an invocation without a deadline timed out", impossible);
        }
    }

    /** Runs an invocation under one absolute, monotonic caller deadline. */
    public static <T> T run(Scheduler scheduler, Duration timeout, Operation<T> operation)
            throws InterruptedException, TimeoutException {
        entryCheck();
        Objects.requireNonNull(scheduler, "scheduler");
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(operation, "operation");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative: " + timeout);
        }

        long started = System.nanoTime();
        long durationNanos;
        try {
            durationNanos = timeout.toNanos();
        } catch (ArithmeticException tooLarge) {
            durationNanos = Long.MAX_VALUE;
        }
        long deadlineNanos = started + durationNanos;
        return execute(scheduler, deadlineNanos, operation);
    }

    /**
     * Poll point for blocking work, such as a filesystem scan, that is not
     * parked in an interruptible JDK primitive when cancellation arrives.
     */
    public static void checkpoint(CancellationSignal signal) throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException("the invocation was interrupted");
        }
        signal.throwIfCancellationRequested();
    }

    private static <T> T execute(Scheduler scheduler, Long deadlineNanos, Operation<T> operation)
            throws InterruptedException, TimeoutException {
        AtomicReference<Outcome> outcome = new AtomicReference<>(Outcome.WAITING);
        CancellationController controller = new CancellationController();
        TimeoutException expired = deadlineNanos == null ? null : new TimeoutException("the caller's deadline expired");
        CountDownLatch timeoutCleanup = new CountDownLatch(deadlineNanos == null ? 0 : 1);
        long remainingNanos = deadlineNanos == null ? Long.MAX_VALUE : deadlineNanos - System.nanoTime();
        if (deadlineNanos != null && remainingNanos <= 0) {
            outcome.set(Outcome.TIMED_OUT);
            timeoutCleanup.countDown();
            controller.close();
            throw expired;
        }
        ScheduledFuture<?> timeoutTask = deadlineNanos == null
                ? null
                : scheduler.schedule(
                        () -> {
                            try {
                                if (outcome.compareAndSet(Outcome.WAITING, Outcome.TIMED_OUT)) {
                                    BoundedCleanup.afterTimeout(controller::cancel, expired);
                                }
                            } finally {
                                timeoutCleanup.countDown();
                            }
                        },
                        remainingNanos,
                        TimeUnit.NANOSECONDS);

        try {
            T result = operation.run(controller.getSignal());
            if (outcome.compareAndSet(Outcome.WAITING, Outcome.COMPLETED)) {
                return result;
            }
            if (outcome.get() == Outcome.TIMED_OUT) {
                awaitTimeoutCleanup(timeoutCleanup);
                throw expired;
            }
            throw new IllegalStateException("the invocation returned after interruption won");
        } catch (Throwable failure) {
            InterruptedException interrupted = interruption(failure);
            if (interrupted != null) {
                if (outcome.compareAndSet(Outcome.WAITING, Outcome.INTERRUPTED)) {
                    BoundedCleanup.afterInterruption(controller::cancel, interrupted);
                    throw interrupted;
                }
                if (outcome.get() == Outcome.TIMED_OUT) {
                    awaitTimeoutCleanup(timeoutCleanup);
                    throw expired;
                }
                throw interrupted;
            }
            if (outcome.compareAndSet(Outcome.WAITING, Outcome.COMPLETED)) {
                if (failure instanceof RuntimeException runtime) {
                    throw runtime;
                }
                if (failure instanceof Error error) {
                    throw error;
                }
                throw new IllegalStateException(failure);
            }
            if (outcome.get() == Outcome.TIMED_OUT) {
                awaitTimeoutCleanup(timeoutCleanup);
                throw expired;
            }
            throw new IllegalStateException("the invocation failed after interruption won", failure);
        } finally {
            if (timeoutTask != null) {
                timeoutTask.cancel(false);
            }
            if (outcome.get() == Outcome.COMPLETED) {
                controller.close();
            }
        }
    }

    private static void entryCheck() throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException("the invocation was interrupted before it started");
        }
    }

    private static void awaitTimeoutCleanup(CountDownLatch cleanup) {
        try {
            cleanup.await(BoundedCleanup.BUDGET_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException secondInterrupt) {
            // Expiry already won. A second cancellation request only shortens
            // cleanup; the caller still observes the winning TimeoutException.
        }
    }

    /**
     * Finds an interrupt preserved under the internal unchecked fault taxonomy.
     *
     * <p>Failures travel unwrapped now, so an interrupt is either the failure
     * itself or sits one cause deep — under the single domain exception a
     * boundary like {@code InterruptedOperationException} names it with. The
     * depth-32 cause walk this replaces existed only to dig interrupts out
     * from under the deleted {@code CompletionException} layers.
     */
    public static InterruptedException interruption(Throwable failure) {
        if (failure instanceof InterruptedException interrupted) {
            return interrupted;
        }
        return failure != null && failure.getCause() instanceof InterruptedException interrupted ? interrupted : null;
    }

    private enum Outcome {
        WAITING,
        COMPLETED,
        TIMED_OUT,
        INTERRUPTED
    }
}
