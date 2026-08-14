// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.transfer;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * How far a transfer has got, and how fast.
 *
 * <p>The rate is smoothed by the library, once, and progress is emitted on a
 * fixed cadence rather than per socket read. Raw per-chunk rates jitter badly
 * enough to be unreadable, and left unsmoothed every consumer smooths them
 * again, differently, and disagrees with every other consumer about how fast the
 * same transfer is going.
 *
 * <p>{@code eta} is optional because it is genuinely unknown at the start of a
 * transfer and whenever the rate falls to zero. Reporting a wrong number there —
 * or reporting {@link Duration#ZERO}, which reads as "finished" — is worse than
 * reporting nothing, since the consumer's alternative is simply to render
 * nothing until an estimate exists.
 *
 * @param transferred bytes transferred so far
 * @param total the expected size in bytes, or {@code 0} if the peer did not say
 * @param bytesPerSecond the smoothed rate
 * @param eta the estimated time remaining, if it can be estimated
 */
public record Progress(long transferred, long total, double bytesPerSecond, Optional<Duration> eta) {

    /**
     * Validates and returns the progress.
     *
     * @throws NullPointerException if {@code eta} is {@code null}
     * @throws IllegalArgumentException if any measure is negative
     */
    public Progress {
        Objects.requireNonNull(eta, "eta");
        if (transferred < 0) {
            throw new IllegalArgumentException("transferred must not be negative: " + transferred);
        }
        if (total < 0) {
            throw new IllegalArgumentException("total must not be negative: " + total);
        }
        if (bytesPerSecond < 0) {
            throw new IllegalArgumentException("bytesPerSecond must not be negative: " + bytesPerSecond);
        }
    }

    /**
     * Returns progress with an estimate derived from the rate.
     *
     * @param transferred bytes transferred so far
     * @param total the expected size, or {@code 0} if unknown
     * @param bytesPerSecond the smoothed rate
     * @return the progress, with an estimate when one can be made
     */
    public static Progress of(long transferred, long total, double bytesPerSecond) {
        return new Progress(transferred, total, bytesPerSecond, estimate(transferred, total, bytesPerSecond));
    }

    /** Progress that has not started. */
    public static Progress none(long total) {
        return new Progress(0, total, 0, Optional.empty());
    }

    private static Optional<Duration> estimate(long transferred, long total, double bytesPerSecond) {
        if (total <= 0 || bytesPerSecond <= 0 || transferred >= total) {
            return Optional.empty();
        }
        double seconds = (total - transferred) / bytesPerSecond;
        if (!Double.isFinite(seconds)) {
            return Optional.empty();
        }
        return Optional.of(Duration.ofMillis(Math.round(seconds * 1000)));
    }

    /**
     * Returns the fraction complete, from {@code 0.0} to {@code 1.0}.
     *
     * <p>Returns {@code 0.0} when the size is unknown, because a progress bar
     * has to render something and a peer that did not state a size gives no
     * basis for a better answer.
     *
     * @return the fraction complete
     */
    public double fraction() {
        if (total <= 0) {
            return 0d;
        }
        return Math.min(1d, (double) transferred / total);
    }

    /**
     * Returns whether every expected byte has arrived.
     *
     * @return {@code true} if complete
     */
    public boolean isComplete() {
        return total > 0 && transferred >= total;
    }
}
