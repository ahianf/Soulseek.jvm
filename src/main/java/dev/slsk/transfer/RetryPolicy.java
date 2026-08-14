// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.transfer;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * When a failed download is worth trying again.
 *
 * <p>The distinction that matters is between a refusal that is about this
 * moment and one that is about this file. A peer whose queue is full will have
 * room later; a peer shutting down will be back. A peer that does not share the
 * file will never share it, and a peer that has banned us will not unban us
 * because we asked eleven more times — retrying those is how a client earns a
 * ban it did not have.
 *
 * <p>So the default retries exactly two rejection reasons and no others. Every
 * other rejection is final on the first answer, which is a policy the protocol
 * justifies rather than a number picked to look cautious.
 *
 * @param maxAttempts how many times to try in total, including the first
 * @param initialBackoff how long to wait before the second attempt
 * @param multiplier how much longer each subsequent wait is
 * @param maxBackoff the longest wait, however many attempts have failed
 * @param retryableRejections which refusals are worth waiting out
 */
public record RetryPolicy(
        int maxAttempts,
        Duration initialBackoff,
        double multiplier,
        Duration maxBackoff,
        Set<RejectionReason> retryableRejections) {

    /** Validates and returns the policy. */
    public RetryPolicy {
        Objects.requireNonNull(initialBackoff, "initialBackoff");
        Objects.requireNonNull(maxBackoff, "maxBackoff");
        retryableRejections = Set.copyOf(Objects.requireNonNull(retryableRejections, "retryableRejections"));
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1: " + maxAttempts);
        }
        if (initialBackoff.isNegative()) {
            throw new IllegalArgumentException("initialBackoff must not be negative: " + initialBackoff);
        }
        if (maxBackoff.compareTo(initialBackoff) < 0) {
            throw new IllegalArgumentException(
                    "maxBackoff must not be shorter than initialBackoff: " + maxBackoff + " < " + initialBackoff);
        }
        if (multiplier < 1) {
            throw new IllegalArgumentException("multiplier must be at least 1: " + multiplier);
        }
    }

    /**
     * Returns the default policy: three attempts, five seconds doubling to five
     * minutes, retrying only a full queue and a pending shutdown.
     *
     * @return the default policy
     */
    public static RetryPolicy defaults() {
        return new RetryPolicy(
                3,
                Duration.ofSeconds(5),
                2.0,
                Duration.ofMinutes(5),
                EnumSet.of(RejectionReason.QUEUE_FULL, RejectionReason.PENDING_SHUTDOWN));
    }

    /**
     * Returns a policy that never retries.
     *
     * @return the policy
     */
    public static RetryPolicy none() {
        return new RetryPolicy(1, Duration.ZERO, 1.0, Duration.ZERO, Set.of());
    }

    /**
     * Returns how long to wait before an attempt.
     *
     * @param attempt which attempt is about to be made, counting the first as 1
     * @return the wait, capped at {@link #maxBackoff}
     */
    public Duration backoffBefore(int attempt) {
        if (attempt <= 1) {
            return Duration.ZERO;
        }
        double scaled = initialBackoff.toMillis() * Math.pow(multiplier, attempt - 2);
        long capped = (long) Math.min(scaled, maxBackoff.toMillis());
        return Duration.ofMillis(Math.max(0, capped));
    }

    /**
     * Returns whether an outcome is worth another attempt.
     *
     * @param outcome how the attempt ended
     * @param attempt which attempt it was, counting the first as 1
     * @return whether to try again
     */
    public boolean shouldRetry(TransferOutcome outcome, int attempt) {
        Objects.requireNonNull(outcome, "outcome");
        if (attempt >= maxAttempts) {
            return false;
        }
        return switch (outcome) {
            case TransferOutcome.Failed failed -> failed.retryable();
            case TransferOutcome.Rejected rejected -> retryableRejections.contains(rejected.reason());
            // A cancellation is what someone asked for, and a success has
            // nothing to try again.
            case TransferOutcome.Succeeded ignored -> false;
            case TransferOutcome.Cancelled ignored -> false;
        };
    }
}
