// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.options.SearchOptions;
import java.util.Objects;

/**
 * Everything one search needs, in one value.
 *
 * <p>Replaces twelve overloads: the cross product of "collect the results" and
 * "stream them to a handler" against four optional arguments.
 *
 * {@snippet :
 * SearchResult result = client.search(
 *         SearchRequest.of(SearchQuery.fromText("miles davis"))
 *                 .scope(SearchScope.network())
 *                 .build());
 * }
 *
 * <p>Which of the two client methods you call decides the shape of the answer:
 * {@code search(request)} blocks and returns the collected {@link SearchResult},
 * while {@code search(request, handler)} streams responses to the handler as
 * they arrive and returns the live {@link Search}.
 */
public final class SearchRequest {

    private final SearchQuery query;
    private final SearchScope scope;
    private final Integer token;
    private final SearchOptions options;
    private final CancellationSignal cancellationSignal;

    private SearchRequest(Builder builder) {
        this.query = builder.query;
        this.scope = builder.scope;
        this.token = builder.token;
        this.options = builder.options;
        this.cancellationSignal = builder.cancellationSignal;
    }

    /**
     * Starts a request for a query.
     *
     * @param query the search query
     * @return a builder
     */
    public static Builder of(SearchQuery query) {
        return new Builder(query);
    }

    /** Returns the query. */
    public SearchQuery getQuery() {
        return query;
    }

    /** Returns the scope, or {@code null} for the network default. */
    public SearchScope getScope() {
        return scope;
    }

    /** Returns the caller-chosen token, or {@code null} to allocate one. */
    public Integer getToken() {
        return token;
    }

    /** Returns the search options, or {@code null} for defaults. */
    public SearchOptions getOptions() {
        return options;
    }

    /** Returns the cancellation signal; never {@code null}. */
    public CancellationSignal getCancellationSignal() {
        return cancellationSignal;
    }

    /** Builds a {@link SearchRequest}. */
    public static final class Builder {
        private final SearchQuery query;
        private SearchScope scope;
        private Integer token;
        private SearchOptions options;
        private CancellationSignal cancellationSignal = CancellationSignal.none();

        private Builder(SearchQuery query) {
            this.query = Objects.requireNonNull(query, "query");
        }

        /** Restricts the search to a scope. */
        public Builder scope(SearchScope scope) {
            this.scope = scope;
            return this;
        }

        /** Sets the search token instead of allocating one. */
        public Builder token(Integer token) {
            this.token = token;
            return this;
        }

        /** Sets the search options. */
        public Builder options(SearchOptions options) {
            this.options = options;
            return this;
        }

        /** Sets the cancellation signal. */
        public Builder cancellation(CancellationSignal cancellationSignal) {
            this.cancellationSignal = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
            return this;
        }

        /** Builds the request. */
        public SearchRequest build() {
            return new SearchRequest(this);
        }
    }
}
