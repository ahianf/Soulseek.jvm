// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.transfer.Progress;
import dev.slsk.transfer.TransferId;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Turns a socket's per-read progress into something a consumer can render.
 *
 * <p>Two problems, and both are the library's rather than the application's.
 *
 * <p><strong>The rate is noise.</strong> Bytes arrive in whatever sizes the
 * network hands over, so a rate computed from one read swings by an order of
 * magnitude between reads. Every consumer that showed it smoothed it, each with
 * its own window, and none of them agreed. Smoothing here is not a convenience:
 * an unsmoothed rate is a wrong number, not an unconfigured one.
 *
 * <p><strong>The events are too many.</strong> A fast transfer produces
 * thousands of reads a second, and a UI that redraws on each is a UI that
 * throttles the network. Progress is emitted on a fixed cadence, so the event
 * rate is bounded by the library rather than by whoever is listening.
 *
 * <p>Both are fixed in 1.0 and configurable in 1.1. The cadence is a number
 * worth arguing about; that there is one is not.
 */
final class ProgressCoalescer {

    /** How often progress is published, per transfer. */
    static final Duration CADENCE = Duration.ofMillis(250);

    /**
     * How much of the rate is the newest sample.
     *
     * <p>Low enough that one slow read does not make the estimate collapse,
     * high enough that a transfer genuinely stalling shows it within a second.
     */
    private static final double ALPHA = 0.25;

    /** What has been seen for one transfer since it started. */
    private static final class Track {
        /**
         * Whether anything has been sampled yet.
         *
         * <p>A flag rather than a sentinel timestamp: a clock is allowed to read
         * zero, and treating zero as "never" made the second reading of every
         * transfer skip its own rate.
         */
        private boolean sampled;

        private long lastBytes;
        private long lastSampleNanos;
        private boolean published;
        private long lastPublishedNanos;
        private double rate;
    }

    private final Map<TransferId, Track> tracks = new ConcurrentHashMap<>();
    private final LongSupplier clock;

    /**
     * Creates a coalescer.
     *
     * @param clock a nanosecond clock; injected so the cadence can be asserted
     *     rather than waited out
     */
    ProgressCoalescer(LongSupplier clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Offers a reading, and returns what to publish if anything.
     *
     * @param id which transfer
     * @param transferred bytes received so far
     * @param total bytes expected
     * @return the progress to publish, or empty if it is not time yet
     */
    Optional<Progress> offer(TransferId id, long transferred, long total) {
        Track track = tracks.computeIfAbsent(id, key -> new Track());
        long now = clock.getAsLong();
        synchronized (track) {
            if (track.sampled) {
                long elapsed = now - track.lastSampleNanos;
                if (elapsed > 0) {
                    double sample = (transferred - track.lastBytes) * 1_000_000_000.0 / elapsed;
                    // The first sample is the rate; there is nothing to blend it
                    // with, and starting from zero would make every transfer
                    // look stalled for its first second.
                    track.rate = track.rate == 0 ? sample : ALPHA * sample + (1 - ALPHA) * track.rate;
                }
            }
            track.sampled = true;
            track.lastBytes = transferred;
            track.lastSampleNanos = now;

            boolean due = !track.published || now - track.lastPublishedNanos >= CADENCE.toNanos();
            // The last byte always publishes: a consumer that never sees 100%
            // renders a progress bar that stops just short of the end.
            boolean complete = total > 0 && transferred >= total;
            if (!due && !complete) {
                return Optional.empty();
            }
            track.published = true;
            track.lastPublishedNanos = now;
            return Optional.of(Progress.of(transferred, total, Math.max(0, track.rate)));
        }
    }

    /** Forgets a transfer that has finished. */
    void forget(TransferId id) {
        tracks.remove(id);
    }
}
