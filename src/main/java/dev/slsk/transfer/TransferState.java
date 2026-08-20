// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.transfer;

import java.time.Instant;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * Where a transfer is right now.
 *
 * <p>This replaces the former internal bit-flag set,
 * in which a state was a combination like {@code COMPLETED | REJECTED} and a
 * consumer had to mask to find out what was true. Two problems came with that.
 * Illegal combinations were representable — nothing stopped {@code QUEUED |
 * SUCCEEDED} — and the data belonging to a state had nowhere to live, so the
 * queue position, the progress and the failure all hung off the transfer whether
 * or not they meant anything in the state it was in.
 *
 * <p>Sealed records fix both. Each state carries exactly the data that state has:
 * a queue position exists only in {@link QueuedRemotely}, progress only in
 * {@link Transferring}, an outcome only in {@link Finished}. A consumer's
 * rendering becomes one {@code switch} the compiler checks for exhaustiveness.
 *
 * <p>The distinction between {@link Queued} and {@link QueuedRemotely} is real
 * and matters to a user: the first is our own queue, which we control and can
 * reorder, and the second is the peer's, which we can only wait in.
 */
public sealed interface TransferState {

    /**
     * Waiting in this library's queue. We have not asked the peer yet.
     *
     * @param localPosition position in our queue, counting from zero
     */
    record Queued(int localPosition) implements TransferState {
        public Queued {
            if (localPosition < 0) {
                throw new IllegalArgumentException("localPosition must not be negative: " + localPosition);
            }
        }
    }

    /** We have asked the peer for the file and are waiting for it to answer. */
    record Requesting() implements TransferState {}

    /**
     * Waiting in the peer's queue.
     *
     * @param position our place, if the peer has told us; peers report this only
     *     when asked, and not all of them answer
     * @param polledAt when the position was last asked for
     */
    record QueuedRemotely(OptionalInt position, Instant polledAt) implements TransferState {
        public QueuedRemotely {
            Objects.requireNonNull(position, "position");
            Objects.requireNonNull(polledAt, "polledAt");
        }
    }

    /**
     * Establishing the transfer connection.
     *
     * @param indirect whether we are going through the server because a direct
     *     connection failed, which is slower and worth showing
     */
    record Connecting(boolean indirect) implements TransferState {}

    /**
     * Bytes are moving.
     *
     * @param progress how far along, and how fast
     */
    record Transferring(Progress progress) implements TransferState {
        public Transferring {
            Objects.requireNonNull(progress, "progress");
        }
    }

    /**
     * Held by the consumer.
     *
     * @param resumeTo the state to return to, so resuming does not have to
     *     re-derive where the transfer was
     */
    record Paused(TransferState resumeTo) implements TransferState {
        public Paused {
            Objects.requireNonNull(resumeTo, "resumeTo");
            if (resumeTo instanceof Paused) {
                throw new IllegalArgumentException("a paused transfer cannot resume to paused");
            }
        }
    }

    /**
     * Over, one way or another.
     *
     * @param outcome how it ended
     */
    record Finished(TransferOutcome outcome) implements TransferState {
        public Finished {
            Objects.requireNonNull(outcome, "outcome");
        }
    }

    /**
     * Returns whether this state is terminal.
     *
     * <p>The idempotent commands lean on this: {@code cancel} on a terminal
     * transfer is a no-op rather than an error, and {@code forget} is refused on
     * anything that is not terminal.
     *
     * @return {@code true} if the transfer is over
     */
    default boolean isTerminal() {
        return this instanceof Finished;
    }

    /**
     * Returns whether the transfer is actively occupying a slot.
     *
     * @return {@code true} if connecting or transferring
     */
    default boolean isActive() {
        return this instanceof Connecting || this instanceof Transferring;
    }
}
