// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import java.util.Objects;

/** A snapshot of a single file search. */
public record SearchSnapshot(
        ParsedSearchQuery query,
        SearchTarget scope,
        int token,
        SearchPhase state,
        SearchTermination termination,
        int responseCount,
        int fileCount,
        int lockedFileCount) {

    public SearchSnapshot {
        state = Objects.requireNonNull(state, "state");
        if ((state == SearchPhase.COMPLETED) != (termination != null)) {
            throw new IllegalArgumentException("only completed searches have a termination reason");
        }
    }
}
