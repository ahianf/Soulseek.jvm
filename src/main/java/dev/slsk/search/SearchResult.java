// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.search;

import dev.slsk.Search;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * A finished search.
 *
 * <p>What {@link Search#run} returns when the search has stopped, by whichever
 * of its limits stopped it.
 *
 * @param id the search
 * @param query what was asked
 * @param status why it stopped
 * @param responses every response kept
 * @param elapsed how long it ran
 */
public record SearchResult(
        SearchId id, SearchQuery query, SearchStatus status, List<SearchResponse> responses, Duration elapsed) {

    /** Validates and returns the result. */
    public SearchResult {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(elapsed, "elapsed");
        responses = List.copyOf(Objects.requireNonNull(responses, "responses"));
    }

    /**
     * Returns whether anybody answered.
     *
     * <p>A search that found nothing is an empty list, not a failure.
     *
     * @return {@code true} if no peer responded
     */
    public boolean isEmpty() {
        return responses.isEmpty();
    }

    /**
     * Returns how many files were offered in total.
     *
     * @return the file count
     */
    public int fileCount() {
        return responses.stream().mapToInt(SearchResponse::fileCount).sum();
    }
}
