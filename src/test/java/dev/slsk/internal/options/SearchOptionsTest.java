// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.search.Search;
import dev.slsk.internal.search.SearchResponse;
import dev.slsk.internal.search.SearchState;
import dev.slsk.internal.share.File;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class SearchOptionsTest {
    @Test
    void instantiatesWithGivenData() {
        SearchResponseFilter responseFilter = response -> false;
        SearchFileFilter fileFilter = file -> true;
        SearchStateChangedCallback stateChanged = change -> {};
        SearchResponseReceivedCallback responseReceived = received -> {};

        SearchOptions options = SearchOptions.builder()
                .searchTimeout(Duration.ofMillis(-1))
                .responseLimit(-2)
                .filterResponses(false)
                .minimumResponseFileCount(-3)
                .maximumPeerQueueLength(-4)
                .minimumPeerUploadSpeed(-5)
                .fileLimit(-6)
                .removeSingleCharacterSearchTerms(false)
                .responseFilter(responseFilter)
                .fileFilter(fileFilter)
                .stateChanged(stateChanged)
                .responseReceived(responseReceived)
                .build();

        assertEquals(Duration.ofMillis(-1), options.searchTimeout());
        assertEquals(-2, options.responseLimit());
        assertFalse(options.filterResponses());
        assertEquals(-3, options.minimumResponseFileCount());
        assertEquals(-4, options.maximumPeerQueueLength());
        assertEquals(-5, options.minimumPeerUploadSpeed());
        assertEquals(-6, options.fileLimit());
        assertFalse(options.removeSingleCharacterSearchTerms());
        assertSame(responseFilter, options.responseFilter());
        assertSame(fileFilter, options.fileFilter());
        assertSame(stateChanged, options.stateChanged());
        assertSame(responseReceived, options.responseReceived());
    }

    @Test
    void usesSourceDefaults() {
        SearchOptions options = new SearchOptions();

        assertEquals(Duration.ofSeconds(15), options.searchTimeout());
        assertEquals(250, options.responseLimit());
        assertTrue(options.filterResponses());
        assertEquals(1, options.minimumResponseFileCount());
        assertEquals(Integer.MAX_VALUE, options.maximumPeerQueueLength());
        assertEquals(0, options.minimumPeerUploadSpeed());
        assertEquals(25_000, options.fileLimit());
        assertTrue(options.removeSingleCharacterSearchTerms());
        assertNull(options.responseFilter());
        assertNull(options.fileFilter());
        assertNull(options.stateChanged());
        assertNull(options.responseReceived());
    }

    @Test
    void builderPreservesEveryUnnamedDefault() {
        SearchOptions options = SearchOptions.builder().responseLimit(2).build();

        assertEquals(Duration.ofSeconds(15), options.searchTimeout());
        assertTrue(options.filterResponses());
        assertEquals(1, options.minimumResponseFileCount());
        assertEquals(Integer.MAX_VALUE, options.maximumPeerQueueLength());
        assertEquals(0, options.minimumPeerUploadSpeed());
        assertEquals(25_000, options.fileLimit());
        assertTrue(options.removeSingleCharacterSearchTerms());
        assertNull(options.responseFilter());
    }

    @Test
    void callbacksAndNamedTupleRecordsAreUsable() {
        AtomicReference<SearchStateChange> state = new AtomicReference<>();
        AtomicReference<SearchResponseReceived> received = new AtomicReference<>();
        SearchOptions options = SearchOptions.builder()
                .searchTimeout(Duration.ofMillis(1))
                .responseLimit(2)
                .minimumResponseFileCount(3)
                .maximumPeerQueueLength(4)
                .minimumPeerUploadSpeed(5)
                .fileLimit(6)
                .responseFilter(response -> response.queueLength() == 7)
                .fileFilter(file -> "x".equals(file.filename()))
                .stateChanged(state::set)
                .responseReceived(received::set)
                .build();
        Search search = new Search(null, null, 9, SearchState.COMPLETED, 0, 0, 0);
        SearchResponse response = new SearchResponse("u", 8, true, 1, 7, List.of());
        File file = new File(1, "x", 2, "ext");

        options.stateChanged().onStateChanged(new SearchStateChange(SearchState.IN_PROGRESS, search));
        options.responseReceived().onResponseReceived(new SearchResponseReceived(search, response));

        assertTrue(options.responseFilter().test(response));
        assertTrue(options.fileFilter().test(file));
        assertEquals(new SearchStateChange(SearchState.IN_PROGRESS, search), state.get());
        assertEquals(new SearchResponseReceived(search, response), received.get());
    }
}
