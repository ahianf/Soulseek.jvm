// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

/** Why a completed search ended. */
public enum SearchTermination {
    /** The caller cancelled the search. */
    CANCELLED,
    /** The response timeout elapsed. */
    TIMED_OUT,
    /** The configured response limit was reached. */
    RESPONSE_LIMIT_REACHED,
    /** The configured file limit was reached. */
    FILE_LIMIT_REACHED,
    /** The search failed. */
    ERRORED
}
