// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.internal.concurrent.CancellationInterrupts;
import dev.slsk.internal.concurrent.CancellationSignal;
import java.util.Objects;
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

        CancellationInterrupts.interruptOnCancel(
                cancellationSignal,
                () -> {
                    lock.lockInterruptibly();
                    return lock;
                },
                ReentrantLock::unlock);
    }
}
