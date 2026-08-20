// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

/** The mutually exclusive lifecycle phase of a search. */
public enum SearchPhase {
    /** No search phase has been assigned. */
    NONE,
    /** The search was requested. */
    REQUESTED,
    /** The search is waiting for the concurrency permit. */
    QUEUED,
    /** The search is accepting responses. */
    IN_PROGRESS,
    /** The search has ended; its termination says why. */
    COMPLETED
}
