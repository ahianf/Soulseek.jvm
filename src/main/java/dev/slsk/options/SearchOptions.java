// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

/** Options for a search operation. */
public class SearchOptions {
    /** Default search timeout in milliseconds. */
    public static final int DEFAULT_SEARCH_TIMEOUT = 15_000;
    /** Default response limit. */
    public static final int DEFAULT_RESPONSE_LIMIT = 250;
    /** Default file limit. */
    public static final int DEFAULT_FILE_LIMIT = 25_000;

    private final SearchFileFilter fileFilter;
    private final int fileLimit;
    private final boolean filterResponses;
    private final int maximumPeerQueueLength;
    private final int minimumPeerUploadSpeed;
    private final int minimumResponseFileCount;
    private final boolean removeSingleCharacterSearchTerms;
    private final SearchResponseFilter responseFilter;
    private final int responseLimit;
    private final SearchResponseReceivedCallback responseReceived;
    private final int searchTimeout;
    private final SearchStateChangedCallback stateChanged;

    /** Creates search options with source defaults. */
    public SearchOptions() {
        this(DEFAULT_SEARCH_TIMEOUT);
    }

    /** Creates options through the search timeout. */
    public SearchOptions(int searchTimeout) {
        this(searchTimeout, DEFAULT_RESPONSE_LIMIT);
    }

    /** Creates options through the response limit. */
    public SearchOptions(int searchTimeout, int responseLimit) {
        this(searchTimeout, responseLimit, true);
    }

    /** Creates options through response filtering. */
    public SearchOptions(int searchTimeout, int responseLimit, boolean filterResponses) {
        this(searchTimeout, responseLimit, filterResponses, 1);
    }

    /** Creates options through the minimum response file count. */
    public SearchOptions(int searchTimeout, int responseLimit, boolean filterResponses, int minimumResponseFileCount) {
        this(searchTimeout, responseLimit, filterResponses, minimumResponseFileCount, Integer.MAX_VALUE);
    }

    /** Creates options through the maximum peer queue length. */
    public SearchOptions(
            int searchTimeout,
            int responseLimit,
            boolean filterResponses,
            int minimumResponseFileCount,
            int maximumPeerQueueLength) {
        this(searchTimeout, responseLimit, filterResponses, minimumResponseFileCount, maximumPeerQueueLength, 0);
    }

    /** Creates options through the minimum peer upload speed. */
    public SearchOptions(
            int searchTimeout,
            int responseLimit,
            boolean filterResponses,
            int minimumResponseFileCount,
            int maximumPeerQueueLength,
            int minimumPeerUploadSpeed) {
        this(
                searchTimeout,
                responseLimit,
                filterResponses,
                minimumResponseFileCount,
                maximumPeerQueueLength,
                minimumPeerUploadSpeed,
                DEFAULT_FILE_LIMIT);
    }

    /** Creates options through the file limit. */
    public SearchOptions(
            int searchTimeout,
            int responseLimit,
            boolean filterResponses,
            int minimumResponseFileCount,
            int maximumPeerQueueLength,
            int minimumPeerUploadSpeed,
            int fileLimit) {
        this(
                searchTimeout,
                responseLimit,
                filterResponses,
                minimumResponseFileCount,
                maximumPeerQueueLength,
                minimumPeerUploadSpeed,
                fileLimit,
                true);
    }

    /** Creates options through single-character-term removal. */
    public SearchOptions(
            int searchTimeout,
            int responseLimit,
            boolean filterResponses,
            int minimumResponseFileCount,
            int maximumPeerQueueLength,
            int minimumPeerUploadSpeed,
            int fileLimit,
            boolean removeSingleCharacterSearchTerms) {
        this(
                searchTimeout,
                responseLimit,
                filterResponses,
                minimumResponseFileCount,
                maximumPeerQueueLength,
                minimumPeerUploadSpeed,
                fileLimit,
                removeSingleCharacterSearchTerms,
                null);
    }

    /** Creates options through the response filter. */
    public SearchOptions(
            int searchTimeout,
            int responseLimit,
            boolean filterResponses,
            int minimumResponseFileCount,
            int maximumPeerQueueLength,
            int minimumPeerUploadSpeed,
            int fileLimit,
            boolean removeSingleCharacterSearchTerms,
            SearchResponseFilter responseFilter) {
        this(
                searchTimeout,
                responseLimit,
                filterResponses,
                minimumResponseFileCount,
                maximumPeerQueueLength,
                minimumPeerUploadSpeed,
                fileLimit,
                removeSingleCharacterSearchTerms,
                responseFilter,
                null);
    }

    /** Creates options through the file filter. */
    public SearchOptions(
            int searchTimeout,
            int responseLimit,
            boolean filterResponses,
            int minimumResponseFileCount,
            int maximumPeerQueueLength,
            int minimumPeerUploadSpeed,
            int fileLimit,
            boolean removeSingleCharacterSearchTerms,
            SearchResponseFilter responseFilter,
            SearchFileFilter fileFilter) {
        this(
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
                null);
    }

    /** Creates options through the state-change callback. */
    public SearchOptions(
            int searchTimeout,
            int responseLimit,
            boolean filterResponses,
            int minimumResponseFileCount,
            int maximumPeerQueueLength,
            int minimumPeerUploadSpeed,
            int fileLimit,
            boolean removeSingleCharacterSearchTerms,
            SearchResponseFilter responseFilter,
            SearchFileFilter fileFilter,
            SearchStateChangedCallback stateChanged) {
        this(
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
                null);
    }

    /** Creates search options. */
    public SearchOptions(
            int searchTimeout,
            int responseLimit,
            boolean filterResponses,
            int minimumResponseFileCount,
            int maximumPeerQueueLength,
            int minimumPeerUploadSpeed,
            int fileLimit,
            boolean removeSingleCharacterSearchTerms,
            SearchResponseFilter responseFilter,
            SearchFileFilter fileFilter,
            SearchStateChangedCallback stateChanged,
            SearchResponseReceivedCallback responseReceived) {
        this.searchTimeout = searchTimeout;
        this.responseLimit = responseLimit;
        this.fileLimit = fileLimit;
        this.filterResponses = filterResponses;
        this.minimumResponseFileCount = minimumResponseFileCount;
        this.maximumPeerQueueLength = maximumPeerQueueLength;
        this.minimumPeerUploadSpeed = minimumPeerUploadSpeed;
        this.responseFilter = responseFilter;
        this.fileFilter = fileFilter;
        this.stateChanged = stateChanged;
        this.responseReceived = responseReceived;
        this.removeSingleCharacterSearchTerms = removeSingleCharacterSearchTerms;
    }

    /** Returns the file filter, or {@code null}. */
    public final SearchFileFilter getFileFilter() {
        return fileFilter;
    }

    /** Returns the maximum accepted file count. */
    public final int getFileLimit() {
        return fileLimit;
    }

    /** Returns whether responses are filtered. */
    public final boolean isFilterResponses() {
        return filterResponses;
    }

    /** Returns the maximum accepted peer queue length. */
    public final int getMaximumPeerQueueLength() {
        return maximumPeerQueueLength;
    }

    /** Returns the minimum accepted peer upload speed. */
    public final int getMinimumPeerUploadSpeed() {
        return minimumPeerUploadSpeed;
    }

    /** Returns the minimum accepted response file count. */
    public final int getMinimumResponseFileCount() {
        return minimumResponseFileCount;
    }

    /** Returns whether single-character terms are removed. */
    public final boolean isRemoveSingleCharacterSearchTerms() {
        return removeSingleCharacterSearchTerms;
    }

    /** Returns the response filter, or {@code null}. */
    public final SearchResponseFilter getResponseFilter() {
        return responseFilter;
    }

    /** Returns the maximum accepted response count. */
    public final int getResponseLimit() {
        return responseLimit;
    }

    /** Returns the response callback, or {@code null}. */
    public final SearchResponseReceivedCallback getResponseReceived() {
        return responseReceived;
    }

    /** Returns the search timeout in milliseconds. */
    public final int getSearchTimeout() {
        return searchTimeout;
    }

    /** Returns the state-change callback, or {@code null}. */
    public final SearchStateChangedCallback getStateChanged() {
        return stateChanged;
    }
}
