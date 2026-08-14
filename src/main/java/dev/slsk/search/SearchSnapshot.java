// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.search;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A search as it stands right now.
 *
 * <p>{@code revision} increases whenever anything changes. It is there so a
 * consumer polling this can answer "did anything happen since I last looked?"
 * with an integer comparison instead of diffing the response list, which for a
 * search with two hundred responses is the difference between a free check and
 * an expensive one.
 *
 * @param id the search
 * @param query what was asked
 * @param status whether it is still running
 * @param startedAt when it began
 * @param endedAt when it stopped, if it has
 * @param responses every response kept, in arrival order
 * @param revision increases on every change
 */
public record SearchSnapshot(
        SearchId id,
        SearchQuery query,
        SearchStatus status,
        Instant startedAt,
        Optional<Instant> endedAt,
        List<SearchResponse> responses,
        long revision) {

    /** Validates and returns the snapshot. */
    public SearchSnapshot {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(endedAt, "endedAt");
        responses = List.copyOf(Objects.requireNonNull(responses, "responses"));
    }

    /**
     * Returns how many peers answered.
     *
     * @return the response count
     */
    public int responseCount() {
        return responses.size();
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
