// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import java.time.Duration;
import java.util.function.Consumer;

/** Options for a search operation. */
public record SearchOptions(
        Duration searchTimeout,
        int responseLimit,
        boolean filterResponses,
        int minimumResponseFileCount,
        int maximumPeerQueueLength,
        int minimumPeerUploadSpeed,
        int fileLimit,
        boolean removeSingleCharacterSearchTerms,
        SearchResponseFilter responseFilter,
        SearchFileFilter fileFilter,
        Consumer<SearchStateChange> stateChanged,
        Consumer<SearchResponseReceived> responseReceived) {
    /** Default search timeout. */
    public static final Duration DEFAULT_SEARCH_TIMEOUT = Duration.ofSeconds(15);
    /** Default response limit. */
    public static final int DEFAULT_RESPONSE_LIMIT = 250;
    /** Default file limit. */
    public static final int DEFAULT_FILE_LIMIT = 25_000;

    /** Creates search options with defaults. */
    public SearchOptions() {
        this(
                DEFAULT_SEARCH_TIMEOUT,
                DEFAULT_RESPONSE_LIMIT,
                true,
                1,
                Integer.MAX_VALUE,
                0,
                DEFAULT_FILE_LIMIT,
                true,
                null,
                null,
                null,
                null);
    }

    /** Starts a field-named search-options builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for search options. */
    public static final class Builder {
        private SearchFileFilter fileFilter;
        private int fileLimit = DEFAULT_FILE_LIMIT;
        private boolean filterResponses = true;
        private int maximumPeerQueueLength = Integer.MAX_VALUE;
        private int minimumPeerUploadSpeed;
        private int minimumResponseFileCount = 1;
        private boolean removeSingleCharacterSearchTerms = true;
        private SearchResponseFilter responseFilter;
        private Consumer<SearchResponseReceived> responseReceived;
        private int responseLimit = DEFAULT_RESPONSE_LIMIT;
        private Duration searchTimeout = DEFAULT_SEARCH_TIMEOUT;
        private Consumer<SearchStateChange> stateChanged;

        public Builder searchTimeout(Duration value) {
            searchTimeout = value;
            return this;
        }

        public Builder responseLimit(int value) {
            responseLimit = value;
            return this;
        }

        public Builder filterResponses(boolean value) {
            filterResponses = value;
            return this;
        }

        public Builder minimumResponseFileCount(int value) {
            minimumResponseFileCount = value;
            return this;
        }

        public Builder maximumPeerQueueLength(int value) {
            maximumPeerQueueLength = value;
            return this;
        }

        public Builder minimumPeerUploadSpeed(int value) {
            minimumPeerUploadSpeed = value;
            return this;
        }

        public Builder fileLimit(int value) {
            fileLimit = value;
            return this;
        }

        public Builder removeSingleCharacterSearchTerms(boolean value) {
            removeSingleCharacterSearchTerms = value;
            return this;
        }

        public Builder responseFilter(SearchResponseFilter value) {
            responseFilter = value;
            return this;
        }

        public Builder fileFilter(SearchFileFilter value) {
            fileFilter = value;
            return this;
        }

        public Builder stateChanged(Consumer<SearchStateChange> value) {
            stateChanged = value;
            return this;
        }

        public Builder responseReceived(Consumer<SearchResponseReceived> value) {
            responseReceived = value;
            return this;
        }

        public SearchOptions build() {
            return new SearchOptions(
                    searchTimeout,
                    responseLimit,
                    filterResponses,
                    minimumResponseFileCount,
                    maximumPeerQueueLength,
                    minimumPeerUploadSpeed,
                    fileLimit,
                    removeSingleCharacterSearchTerms,
                    responseFilter,
                    fileFilter,
                    stateChanged,
                    responseReceived);
        }
    }
}
