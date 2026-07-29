// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A one-shot cell: settled once, awaited by many, and read as a failure or not.
 *
 * <p>Written for the transfer path, and named for it until a search wanted the
 * same thing. A transfer in flight can end in three unrelated ways at the same
 * time.
 * The bytes finish moving; the transfer connection drops under them; the peer
 * sends {@code UploadFailed} or {@code UploadDenied} on an entirely different
 * connection. Each is reported by a different thread — the transfer's own, the
 * connection's disconnect callback, and a peer read loop — and the first to
 * arrive is the answer. That is genuine concurrency, and it is the reason the
 * transfer path is one of the few places a second thread earns its keep.
 *
 * <p>A search's completion is the second use and a different shape of the same
 * need: one terminal state, every waiter, and a caller that can abandon its own
 * wait without ending the search. That is one of these per waiter, settled
 * together — see {@code SearchInternal.waitForCompletion}.
 *
 * <p>It was three {@link java.util.concurrent.CompletableFuture}s and a
 * {@code CompletableFuture.anyOf} over them. What the futures provided was a
 * one-shot cell that blocks a reader until somebody writes it, so that is what
 * this is: settle once, win once, everybody else is a no-op, and the waiter
 * gets whatever the winner said.
 *
 * <p>{@link #succeed()} and {@link #fail(Throwable)} are the two ways to
 * settle, and both report whether this call was the one that did it. Nothing in
 * the transfer path needs to know, but a caller that must undo work on losing
 * can.
 */
public final class Settlement {

    private final Object lock = new Object();
    private final CountDownLatch settled = new CountDownLatch(1);
    private boolean done;
    private Throwable failure;

    /**
     * Settles as complete.
     *
     * @return whether this call was the one that settled it
     */
    public boolean succeed() {
        return settle(null);
    }

    /**
     * Settles with a failure.
     *
     * @param cause what went wrong; never {@code null}
     * @return whether this call was the one that settled it
     */
    public boolean fail(Throwable cause) {
        return settle(Objects.requireNonNull(cause, "cause"));
    }

    private boolean settle(Throwable cause) {
        synchronized (lock) {
            if (done) {
                return false;
            }
            done = true;
            failure = cause;
        }
        settled.countDown();
        return true;
    }

    /** Returns whether this has been settled either way. */
    public boolean isSettled() {
        synchronized (lock) {
            return done;
        }
    }

    /**
     * Returns the failure this settled with, or {@code null}.
     *
     * @return the failure, or {@code null} if it succeeded or has not settled
     */
    public Throwable failure() {
        synchronized (lock) {
            return failure;
        }
    }

    /**
     * Waits for the settlement.
     *
     * <p>Uninterruptibly, restoring the interrupt on the way out — the same
     * rule the rest of this library's blocking waits follow. An interrupt
     * belongs to whatever the caller does next; a transfer is stopped through
     * its {@link dev.slsk.CancellationSignal}, not through the waiting
     * thread.
     *
     * @return the failure it settled with, or {@code null} if it succeeded
     */
    public Throwable await() {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    settled.await();
                    return failure();
                } catch (InterruptedException exception) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Waits for the settlement, giving up after a deadline.
     *
     * @param milliseconds how long to wait
     * @return whether it settled within the deadline
     */
    public boolean await(long milliseconds) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0, milliseconds));
        boolean interrupted = false;
        try {
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return isSettled();
                }
                try {
                    return settled.await(remaining, TimeUnit.NANOSECONDS);
                } catch (InterruptedException exception) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
