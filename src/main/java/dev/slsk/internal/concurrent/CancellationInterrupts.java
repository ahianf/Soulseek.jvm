// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.concurrent;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;

/** Bridges an internal cancellation signal to one interruptible JDK wait. */
public final class CancellationInterrupts {

    private CancellationInterrupts() {}

    /** Runs an interruptible operation and compensates if cancellation wins after it completes. */
    public static <T> T interruptOnCancel(
            CancellationSignal signal, InterruptibleSupplier<T> operation, Consumer<? super T> compensate)
            throws InterruptedException {
        Objects.requireNonNull(signal, "signal");
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(compensate, "compensate");
        if (Thread.interrupted()) {
            throw new InterruptedException("The wait was interrupted before it started");
        }
        signal.throwIfCancellationRequested();
        if (signal == CancellationSignal.none()) {
            return operation.get();
        }

        Bridge bridge = new Bridge(Thread.currentThread());
        T result = null;
        boolean completed = false;
        InterruptedException interrupted = null;
        boolean cancelled;
        try (CancellationSubscription ignored = signal.register(bridge::cancel)) {
            try {
                result = operation.get();
                completed = true;
            } catch (InterruptedException failure) {
                interrupted = failure;
            } finally {
                cancelled = bridge.finish();
            }
        }

        if (cancelled) {
            // Cancellation's interrupt is an implementation detail. The
            // CancellationException is the signal observed by internal code.
            Thread.interrupted();
            if (completed) {
                compensate.accept(result);
            }
            throw new CancellationException("The operation was cancelled");
        }
        if (interrupted != null) {
            throw interrupted;
        }
        return result;
    }

    @FunctionalInterface
    public interface InterruptibleSupplier<T> {
        T get() throws InterruptedException;
    }

    private static final class Bridge {
        private final Thread waiter;
        private boolean waiting = true;
        private boolean cancelled;

        private Bridge(Thread waiter) {
            this.waiter = waiter;
        }

        private synchronized void cancel() {
            if (waiting) {
                cancelled = true;
                waiter.interrupt();
            }
        }

        private synchronized boolean finish() {
            waiting = false;
            return cancelled;
        }
    }
}
