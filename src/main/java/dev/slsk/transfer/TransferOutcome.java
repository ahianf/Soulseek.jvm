// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.transfer;

import java.time.Duration;
import java.util.Objects;

/**
 * How a transfer ended.
 *
 * <p>This is a value, not an exception, and that is the point. A peer refusing a
 * file is an ordinary thing that happens dozens of times in a session: the file
 * moved, the queue is full, we are not a privileged user today. Modelling it as
 * a thrown {@code TransferRejectedException} forces every consumer to wrap every
 * call and turns a normal outcome into control flow. Exceptions stay for faults
 * — not connected, protocol violation, I/O failure — and outcomes are returned.
 *
 * <p>Sealed, so a consumer's rendering is one {@code switch} the compiler checks.
 */
public sealed interface TransferOutcome {

    /**
     * The file arrived complete.
     *
     * @param bytes how many bytes were transferred
     * @param elapsed how long the transfer took, from first byte to last
     */
    record Succeeded(long bytes, Duration elapsed) implements TransferOutcome {
        public Succeeded {
            Objects.requireNonNull(elapsed, "elapsed");
            if (bytes < 0) {
                throw new IllegalArgumentException("bytes must not be negative: " + bytes);
            }
        }
    }

    /** We cancelled it. Not a failure, and never retried. */
    record Cancelled() implements TransferOutcome {}

    /**
     * The peer refused.
     *
     * @param reason the classification
     * @param rawMessage what the peer actually said, kept verbatim so a consumer
     *     can show it when {@code reason} is {@link RejectionReason#UNKNOWN} and
     *     so a new reason can be recognised later without having lost the text
     */
    record Rejected(RejectionReason reason, String rawMessage) implements TransferOutcome {
        public Rejected {
            Objects.requireNonNull(reason, "reason");
            Objects.requireNonNull(rawMessage, "rawMessage");
        }
    }

    /**
     * Something went wrong on our side or on the wire.
     *
     * @param cause what went wrong
     * @param retryable whether trying again could plausibly succeed. A timeout
     *     or a dropped connection is retryable; a malformed message is not.
     */
    record Failed(Throwable cause, boolean retryable) implements TransferOutcome {
        public Failed {
            Objects.requireNonNull(cause, "cause");
        }
    }

    /**
     * Returns whether the file arrived complete.
     *
     * @return {@code true} for {@link Succeeded}
     */
    default boolean isSuccess() {
        return this instanceof Succeeded;
    }
}
