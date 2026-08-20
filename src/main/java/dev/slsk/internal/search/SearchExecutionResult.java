// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import java.util.List;
import java.util.Objects;

/**
 * The completed search context and collected responses.
 *
 * <p>This record is the named Java equivalent of the C# method's
 * {@code (SearchSnapshot SearchSnapshot, IReadOnlyCollection<SearchResponseMessage> Responses)}
 * tuple.</p>
 *
 * @param search the completed search snapshot
 * @param responses the accepted search responses
 */
public record SearchExecutionResult(SearchSnapshot search, List<SearchResponseMessage> responses) {
    /** Creates an immutable search result. */
    public SearchExecutionResult {
        Objects.requireNonNull(search, "search");
        responses = List.copyOf(Objects.requireNonNull(responses, "responses"));
    }
}
