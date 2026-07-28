// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.time.Duration;
import java.util.Objects;

/**
 * When to stop waiting for search responses.
 *
 * <p><strong>The idle timeout is the load-bearing one.</strong> Soulseek never
 * signals that a search is complete, so completion is a policy a client has to
 * invent. Waiting the full overall timeout every time feels broken to a user
 * watching results stop after three seconds; stopping when the trickle stops is
 * what makes the spinner honest.
 *
 * @param overall the hard stop, however many responses are arriving
 * @param idle how long a silence has to last before the search is considered done
 * @param maxResponses stop after this many peers have answered
 * @param maxFilesPerUser ignore files beyond this many from one peer
 */
public record SearchLimits(Duration overall, Duration idle, int maxResponses, int maxFilesPerUser) {

    private static final SearchLimits DEFAULTS =
            new SearchLimits(Duration.ofSeconds(15), Duration.ofSeconds(4), 250, 500);

    /** Validates and returns the limits. */
    public SearchLimits {
        Objects.requireNonNull(overall, "overall");
        Objects.requireNonNull(idle, "idle");
        if (overall.isNegative() || overall.isZero()) {
            throw new IllegalArgumentException("overall must be positive: " + overall);
        }
        if (idle.isNegative() || idle.isZero()) {
            throw new IllegalArgumentException("idle must be positive: " + idle);
        }
        if (maxResponses < 1) {
            throw new IllegalArgumentException("maxResponses must be positive: " + maxResponses);
        }
        if (maxFilesPerUser < 1) {
            throw new IllegalArgumentException("maxFilesPerUser must be positive: " + maxFilesPerUser);
        }
    }

    /**
     * Returns limits that suit an interactive search.
     *
     * @return 15s overall, 4s idle, 250 responses, 500 files per peer
     */
    public static SearchLimits defaults() {
        return DEFAULTS;
    }
}
