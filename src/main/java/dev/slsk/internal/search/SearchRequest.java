// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.options.SearchOptions;
import java.util.Objects;

/** Everything one search needs, in one value. */
public record SearchRequest(
        SearchQuery query,
        SearchScope scope,
        Integer token,
        SearchOptions options,
        CancellationSignal cancellationSignal) {

    public SearchRequest {
        query = Objects.requireNonNull(query, "query");
        cancellationSignal = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
    }

    private SearchRequest(Builder builder) {
        this(builder.query, builder.scope, builder.token, builder.options, builder.cancellationSignal);
    }

    public static Builder of(SearchQuery query) {
        return new Builder(query);
    }

    public static final class Builder {
        private final SearchQuery query;
        private SearchScope scope;
        private Integer token;
        private SearchOptions options;
        private CancellationSignal cancellationSignal = CancellationSignal.none();

        private Builder(SearchQuery query) {
            this.query = Objects.requireNonNull(query, "query");
        }

        public Builder scope(SearchScope scope) {
            this.scope = scope;
            return this;
        }

        public Builder token(Integer token) {
            this.token = token;
            return this;
        }

        public Builder options(SearchOptions options) {
            this.options = options;
            return this;
        }

        public Builder cancellation(CancellationSignal cancellationSignal) {
            this.cancellationSignal = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
            return this;
        }

        public SearchRequest build() {
            return new SearchRequest(this);
        }
    }
}
