// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.concurrent.CancellationSubscription;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/** Cancellation-aware acquisition of an interruptible JDK lock. */
public final class Locks {

    private Locks() {}

    /** Acquires {@code lock}, interrupting its wait when {@code cancellationSignal} fires. */
    public static void acquire(ReentrantLock lock, CancellationSignal cancellationSignal) throws InterruptedException {
        Objects.requireNonNull(lock, "lock");
        Objects.requireNonNull(cancellationSignal, "cancellationSignal");
        if (Thread.interrupted()) {
            throw new InterruptedException("The lock wait was interrupted before it started");
        }
        cancellationSignal.throwIfCancellationRequested();
        if (lock.tryLock()) {
            return;
        }

        Thread waiter = Thread.currentThread();
        AtomicReference<Outcome> outcome = new AtomicReference<>(Outcome.WAITING);
        try (CancellationSubscription registration = cancellationSignal.register(() -> {
            if (outcome.compareAndSet(Outcome.WAITING, Outcome.CANCELLED)) {
                waiter.interrupt();
            }
        })) {
            lock.lockInterruptibly();
            if (!outcome.compareAndSet(Outcome.WAITING, Outcome.ACQUIRED)) {
                lock.unlock();
                throw new CancellationException("The operation was cancelled");
            }
        } catch (InterruptedException interrupted) {
            if (outcome.compareAndSet(Outcome.WAITING, Outcome.INTERRUPTED)) {
                throw interrupted;
            }
            if (outcome.get() == Outcome.CANCELLED) {
                throw new CancellationException("The operation was cancelled");
            }
            Thread.currentThread().interrupt();
        }
    }

    private enum Outcome {
        WAITING,
        ACQUIRED,
        CANCELLED,
        INTERRUPTED
    }
}
