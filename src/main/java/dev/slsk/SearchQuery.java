// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.Objects;

/**
 * What to search for, where, and when to stop.
 *
 * @param terms the search text
 * @param scope who to ask
 * @param limits when to stop waiting
 * @param filters which responses to keep
 */
public record SearchQuery(String terms, SearchScope scope, SearchLimits limits, SearchFilters filters) {

    /** Validates and returns the query. */
    public SearchQuery {
        Objects.requireNonNull(terms, "terms");
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(filters, "filters");
        if (terms.isBlank()) {
            throw new IllegalArgumentException("terms must not be blank");
        }
    }

    /**
     * Returns a network search with default limits and no filters.
     *
     * @param terms what to search for
     * @return the query
     */
    public static SearchQuery of(String terms) {
        return new SearchQuery(terms, SearchScope.network(), SearchLimits.defaults(), SearchFilters.none());
    }

    /**
     * Returns a copy with a different scope.
     *
     * @param value the scope
     * @return the updated query
     */
    public SearchQuery withScope(SearchScope value) {
        return new SearchQuery(terms, value, limits, filters);
    }

    /**
     * Returns a copy with different limits.
     *
     * @param value the limits
     * @return the updated query
     */
    public SearchQuery withLimits(SearchLimits value) {
        return new SearchQuery(terms, scope, value, filters);
    }

    /**
     * Returns a copy with different filters.
     *
     * @param value the filters
     * @return the updated query
     */
    public SearchQuery withFilters(SearchFilters value) {
        return new SearchQuery(terms, scope, limits, value);
    }
}
