// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.search;

/**
 * Whether a search is still running, and if not, why it stopped.
 *
 * <p>Soulseek never says a search is finished. Responses simply stop arriving,
 * and there is no message meaning "that was all of them". Every terminal state
 * here is therefore a policy this library applies rather than something the
 * network reported.
 */
public enum SearchStatus {

    /** Still accepting responses. */
    IN_PROGRESS,

    /** Stopped because responses stopped arriving, or a limit was reached. */
    COMPLETED,

    /** Stopped because the caller cancelled it. */
    CANCELLED,

    /** Stopped because the overall time limit expired. */
    TIMED_OUT;

    /**
     * Returns whether the search has stopped.
     *
     * @return {@code true} unless in progress
     */
    public boolean isTerminal() {
        return this != IN_PROGRESS;
    }
}
