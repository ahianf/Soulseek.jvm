// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
public final class Settlement<T> {

    private final CountDownLatch settled = new CountDownLatch(1);
    private final AtomicReference<Outcome<T>> outcome = new AtomicReference<>();

    /**
     * Settles as complete.
     *
     * @return whether this call was the one that settled it
     */
    public boolean succeed() {
        return succeed(null);
    }

    /** Settles successfully with a payload. */
    public boolean succeed(T value) {
        return settle(value, null);
    }

    /**
     * Settles with a failure.
     *
     * @param cause what went wrong; never {@code null}
     * @return whether this call was the one that settled it
     */
    public boolean fail(Throwable cause) {
        return settle(null, Objects.requireNonNull(cause, "cause"));
    }

    /** Commits an outcome without publishing it to waiters yet. */
    public boolean trySettle(T value, Throwable failure) {
        if (failure != null) {
            Objects.requireNonNull(failure, "failure");
        }
        return outcome.compareAndSet(null, new Outcome<>(value, failure));
    }

    /** Publishes an outcome previously committed with {@link #trySettle}. */
    public void publish() {
        if (outcome.get() == null) {
            throw new IllegalStateException("No settlement outcome has been committed");
        }
        settled.countDown();
    }

    private boolean settle(T value, Throwable failure) {
        if (!trySettle(value, failure)) {
            return false;
        }
        publish();
        return true;
    }

    /** Returns whether this has been settled either way. */
    public boolean isSettled() {
        return outcome.get() != null;
    }

    /** Returns the committed outcome, or {@code null} while unsettled. */
    public Outcome<T> outcome() {
        return outcome.get();
    }

    /** Returns the committed failure, or {@code null} before or after success. */
    public Throwable failure() {
        Outcome<T> current = outcome();
        return current == null ? null : current.failure();
    }

    /**
     * Waits for the settlement.
     *
     * @return the immutable winning outcome
     * @throws InterruptedException if the waiting thread is interrupted first
     */
    public Outcome<T> await() throws InterruptedException {
        try {
            settled.await();
        } catch (InterruptedException interrupted) {
            if (isSettled()) {
                Thread.currentThread().interrupt();
                awaitPublished();
                return outcome();
            }
            throw interrupted;
        }
        return outcome();
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

    private void awaitPublished() {
        boolean interrupted = false;
        while (settled.getCount() != 0) {
            try {
                settled.await();
            } catch (InterruptedException repeated) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** The immutable result committed by the winning settler. */
    public record Outcome<T>(T value, Throwable failure) {}
}
