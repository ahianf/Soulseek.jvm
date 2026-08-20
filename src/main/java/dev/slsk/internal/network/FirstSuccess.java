// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.internal.common.Failures;
import dev.slsk.internal.network.tcp.Connection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Races two attempts to reach a peer and returns the first that succeeds.
 *
 * <p>Every peer connection this library makes is attempted two ways at once:
 * directly to the endpoint the server gave us, and indirectly by asking the
 * server to have the peer connect back to us. Either can legitimately fail — a
 * firewalled peer never answers the direct attempt, a peer that has gone
 * offline never answers the indirect one — so the pair is raced, and the race
 * is lost only when <em>both</em> arms fail.
 *
 * <p>This is one of the two places in this library where a second thread buys
 * something a blocking call cannot: two attempts genuinely have to be in flight
 * at the same time. A virtual thread apiece, a one-slot handoff, and the caller
 * blocks on the handoff. It replaces a private {@code firstSuccessful}
 * combinator over futures that the two connection managers held a copy of
 * each, and whose only remaining purpose was to express that rule.
 *
 * <p><strong>The arm that arrives second is closed.</strong> It is a live
 * socket to a peer that nobody will ever read from, and the combinator this
 * replaces simply dropped it: whenever both arms succeeded, the loser's
 * connection leaked. Callers still cancel the losing arm once they know who
 * won; that stops an attempt still in flight, which is a different job from
 * closing one that finished.
 */
final class FirstSuccess {

    private FirstSuccess() {}

    /**
     * Runs both attempts on their own threads and returns the first to succeed.
     *
     * <p>Blocks until one arm succeeds or both have failed. The failure that
     * ends a lost race is raised as itself — nothing arrives wrapped.
     *
     * @param first the first attempt; the direct one at every call site
     * @param second the second attempt; the indirect one at every call site
     * @param <T> the connection type both attempts produce
     * @return the winning arm and its connection
     * @throws InterruptedException if the caller abandons the race
     * @throws TimeoutException if the arm that lost the race timed out
     */
    static <T extends Connection> Winner<T> race(Attempt<T> first, Attempt<T> second)
            throws InterruptedException, TimeoutException {
        return race(
                command -> Thread.ofVirtual().name("soulseek-standalone-race").start(command), first, second);
    }

    /** Runs both attempts on the supplied client executor. */
    static <T extends Connection> Winner<T> race(Executor executor, Attempt<T> first, Attempt<T> second)
            throws InterruptedException, TimeoutException {
        // One slot, because exactly one outcome is ever offered: the first arm
        // to succeed, or the second arm to fail.
        BlockingQueue<Outcome<T>> handoff = new ArrayBlockingQueue<>(1);
        AtomicReference<Throwable> lostArm = new AtomicReference<>();
        AtomicBoolean won = new AtomicBoolean();

        executor.execute(() -> attempt(first, true, handoff, lostArm, won));
        executor.execute(() -> attempt(second, false, handoff, lostArm, won));

        Outcome<T> outcome = take(handoff);
        if (outcome.failure() != null) {
            throw Failures.rethrow(outcome.failure());
        }
        return outcome.winner();
    }

    private static <T extends Connection> void attempt(
            Attempt<T> arm,
            boolean first,
            BlockingQueue<Outcome<T>> handoff,
            AtomicReference<Throwable> lostArm,
            AtomicBoolean won) {
        T value;
        try {
            value = arm.get();
        } catch (Throwable failure) {
            // The race is lost only once both arms have failed, and the failure
            // the caller sees is the one that ended it.
            if (lostArm.getAndSet(failure) != null) {
                handoff.offer(new Outcome<>(null, failure));
            }
            return;
        }

        if (won.compareAndSet(false, true)) {
            handoff.offer(new Outcome<>(new Winner<>(value, first), null));
        } else {
            value.close();
        }
    }

    /** Waits interruptibly for the first completed arm. */
    private static <T> T take(BlockingQueue<T> handoff) throws InterruptedException {
        return handoff.take();
    }

    @FunctionalInterface
    interface Attempt<T extends Connection> {
        T get() throws Exception;
    }

    /**
     * The arm that won a race, and what it produced.
     *
     * @param value the winning attempt's connection
     * @param first whether the winner was the first attempt passed to
     *     {@link #race}, which is the direct one at every call site
     * @param <T> the connection type
     */
    record Winner<T extends Connection>(T value, boolean first) {}

    private record Outcome<T extends Connection>(Winner<T> winner, Throwable failure) {}
}
