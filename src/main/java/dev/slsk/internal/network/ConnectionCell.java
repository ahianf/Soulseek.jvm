// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.Settlement;
import java.util.concurrent.TimeoutException;

/**
 * One peer's message connection, and the establishment that produces it.
 *
 * <p>A connection cache entry is claimed before the connection exists. That is
 * the point of it: the first caller to ask for a peer puts a cell in the map
 * and goes off to establish it, and every caller that arrives while it is
 * still in flight finds the cell and waits on it rather than opening a second
 * socket to the same peer. One establishment, N waiters, one result — the two
 * properties a {@code CompletableFuture} was carrying here, without the
 * composition that came with it.
 *
 * <p>Whoever claimed the cell settles it exactly once, with a connection or
 * with the failure that stopped it, and the latch publishes that to everyone
 * waiting. {@link #await()} raises the stored failure as itself — nothing
 * arrives wrapped.
 *
 * <p>{@link #peek()} is the other half of the reason this is not a future.
 * Counting the connections to peers must never be the thing that waits for one,
 * and neither must deciding whether a disconnected connection is still the one
 * on record; both ask a settled cell for its value and take {@code null} for an
 * answer.
 */
final class ConnectionCell {
    private final Settlement<MessageConnection> settlement = new Settlement<>();
    private boolean disposed;

    /**
     * Waits for this cell to settle and returns its connection.
     *
     * @return the established connection
     * @throws InterruptedException if this caller abandons its wait; the shared
     *     establishment continues
     * @throws TimeoutException if the establishment timed out
     */
    MessageConnection await() throws InterruptedException, TimeoutException {
        Settlement.Outcome<MessageConnection> outcome = settlement.await();
        Throwable cause = outcome.failure();
        if (cause != null) {
            throw Failures.rethrow(cause);
        }
        return outcome.value();
    }

    /**
     * Waits for this cell to settle and returns its connection, or {@code null}
     * if the establishment failed.
     *
     * @return the established connection, or {@code null}
     */
    MessageConnection awaitQuietly() throws InterruptedException {
        Settlement.Outcome<MessageConnection> outcome = settlement.await();
        return outcome.failure() == null ? outcome.value() : null;
    }

    /**
     * Returns the established connection without waiting for one.
     *
     * @return the connection, or {@code null} if this cell has not settled or
     *     settled on a failure
     */
    synchronized MessageConnection peek() {
        Settlement.Outcome<MessageConnection> outcome = settlement.outcome();
        return outcome != null && outcome.failure() == null ? outcome.value() : null;
    }

    /**
     * Hands over the established connection.
     *
     * @param value the connection
     */
    void settle(MessageConnection value) {
        boolean close;

        synchronized (this) {
            if (!settlement.succeed(value)) {
                return;
            }
            close = disposed;
        }

        // Claimed while it was still being established. The caller that
        // established it still receives it, exactly as it received a connection
        // closed by the future this replaces.
        if (close && value != null) {
            value.close();
        }
    }

    /**
     * Hands over the failure that stopped the establishment.
     *
     * <p>Whichever comes first wins and the rest are ignored, so a caller that
     * lost a race to settle this cell cannot overwrite what the waiters already
     * saw.
     *
     * @param cause the failure
     */
    void fail(Throwable cause) {
        settlement.fail(cause);
    }

    /**
     * Closes this cell's connection, whenever it arrives.
     *
     * <p>What disposing of a cache does about the attempts still in it. An
     * establishment in flight owns a socket that nobody asked for any more, and
     * it cannot be closed until it exists.
     */
    void closeWhenSettled() {
        MessageConnection close;

        synchronized (this) {
            disposed = true;
            Settlement.Outcome<MessageConnection> outcome = settlement.outcome();
            close = outcome != null && outcome.failure() == null ? outcome.value() : null;
        }

        if (close != null) {
            close.close();
        }
    }
}
