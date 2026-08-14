// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.download;

import dev.slsk.transfer.Bandwidth;
import dev.slsk.transfer.RetryPolicy;
import java.time.Duration;
import java.util.Objects;

/**
 * How the library runs the download queue.
 *
 * <p>{@code maxConcurrentPerUser} is the field that matters most, and it
 * defaults to one. Opening four connections to the same peer to fetch four
 * tracks of the same album is how clients get banned: from the peer's side it
 * is indistinguishable from an attack, and their client will treat it as one.
 * It is exactly the kind of courtesy rule that belongs in a library that knows
 * the protocol rather than in each application that uses it, because an
 * application has no reason to know it and every reason to want the album
 * faster.
 *
 * <p><strong>These bound transfers, not queueing.</strong> The distinction is
 * the whole reason an album arrives at a sensible speed. Wanting a file from a
 * peer costs one line in that peer's queue, sent over the message connection
 * that peer already has; it opens nothing, so there is nothing for a ceiling to
 * protect and every one of an album's tracks is asked for at once. Only when a
 * peer says it is ready does a transfer connection exist, and that is where
 * these two apply.
 *
 * <p>The consequence is that the peer, not this policy, decides when a download
 * starts — there is no way to decline an offer and keep your place in the queue
 * that made it, so the library takes what it is given. What throttles queueing
 * instead is the peer's own refusal: a peer that will hold no more files says
 * so, and the library asks for less until its queue drains. That is reactive by
 * necessity, because the protocol has no way to ask a peer how much it will
 * hold.
 *
 * @param maxConcurrent how many downloads transfer at once, across all peers
 * @param maxConcurrentPerUser how many transfer at once from any one peer
 * @param speedLimit the aggregate download rate ceiling
 * @param queuePositionPollInterval how often to ask a peer where we are in its
 *     queue
 * @param retry when a failed download is worth trying again
 */
public record DownloadPolicy(
        int maxConcurrent,
        int maxConcurrentPerUser,
        Bandwidth speedLimit,
        Duration queuePositionPollInterval,
        RetryPolicy retry) {

    /** Validates and returns the policy. */
    public DownloadPolicy {
        Objects.requireNonNull(speedLimit, "speedLimit");
        Objects.requireNonNull(queuePositionPollInterval, "queuePositionPollInterval");
        Objects.requireNonNull(retry, "retry");
        if (maxConcurrent < 1) {
            throw new IllegalArgumentException("maxConcurrent must be at least 1: " + maxConcurrent);
        }
        if (maxConcurrentPerUser < 1) {
            throw new IllegalArgumentException("maxConcurrentPerUser must be at least 1: " + maxConcurrentPerUser);
        }
        if (maxConcurrentPerUser > maxConcurrent) {
            throw new IllegalArgumentException("maxConcurrentPerUser must not exceed maxConcurrent: "
                    + maxConcurrentPerUser + " > " + maxConcurrent);
        }
        if (queuePositionPollInterval.isNegative() || queuePositionPollInterval.isZero()) {
            throw new IllegalArgumentException(
                    "queuePositionPollInterval must be positive: " + queuePositionPollInterval);
        }
    }

    /**
     * Returns the default policy: three at once, one per peer, unlimited rate,
     * polling each peer's queue every thirty seconds.
     *
     * @return the default policy
     */
    public static DownloadPolicy defaults() {
        return new DownloadPolicy(3, 1, Bandwidth.unlimited(), Duration.ofSeconds(30), RetryPolicy.defaults());
    }

    /**
     * Returns this policy with a different overall concurrency.
     *
     * @param value how many downloads transfer at once
     * @return the policy
     */
    public DownloadPolicy maxConcurrent(int value) {
        return new DownloadPolicy(
                value, Math.min(maxConcurrentPerUser, value), speedLimit, queuePositionPollInterval, retry);
    }

    /**
     * Returns this policy with a different per-peer concurrency.
     *
     * @param value how many downloads transfer at once from any one peer
     * @return the policy
     */
    public DownloadPolicy maxConcurrentPerUser(int value) {
        return new DownloadPolicy(maxConcurrent, value, speedLimit, queuePositionPollInterval, retry);
    }

    /**
     * Returns this policy with a different rate ceiling.
     *
     * @param value the aggregate download rate ceiling
     * @return the policy
     */
    public DownloadPolicy speedLimit(Bandwidth value) {
        return new DownloadPolicy(maxConcurrent, maxConcurrentPerUser, value, queuePositionPollInterval, retry);
    }

    /**
     * Returns this policy with a different queue-position poll interval.
     *
     * @param value how often to ask a peer where we are in its queue
     * @return the policy
     */
    public DownloadPolicy queuePositionPollInterval(Duration value) {
        return new DownloadPolicy(maxConcurrent, maxConcurrentPerUser, speedLimit, value, retry);
    }

    /**
     * Returns this policy with a different retry policy.
     *
     * @param value when a failed download is worth trying again
     * @return the policy
     */
    public DownloadPolicy retry(RetryPolicy value) {
        return new DownloadPolicy(maxConcurrent, maxConcurrentPerUser, speedLimit, queuePositionPollInterval, value);
    }
}
