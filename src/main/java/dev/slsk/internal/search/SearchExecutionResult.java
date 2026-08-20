// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import java.util.List;
import java.util.Objects;

/**
 * The completed search context and collected responses.
 *
 * <p>The named result keeps the completed snapshot and the responses accepted
 * for it together without exposing mutable collection state.
 *
 * @param search the completed search snapshot
 * @param responses the accepted search responses
 */
public record SearchExecutionResult(SearchStateSnapshot search, List<SearchResponseMessage> responses) {
    /** Creates an immutable search result. */
    public SearchExecutionResult {
        Objects.requireNonNull(search, "search");
        responses = List.copyOf(Objects.requireNonNull(responses, "responses"));
    }
}
