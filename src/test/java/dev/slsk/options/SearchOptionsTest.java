// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.File;
import dev.slsk.Search;
import dev.slsk.SearchResponse;
import dev.slsk.SearchStates;
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

        SearchOptions options = new SearchOptions(
                -1, -2, false, -3, -4, -5, -6, false, responseFilter, fileFilter, stateChanged, responseReceived);

        assertEquals(-1, options.getSearchTimeout());
        assertEquals(-2, options.getResponseLimit());
        assertFalse(options.isFilterResponses());
        assertEquals(-3, options.getMinimumResponseFileCount());
        assertEquals(-4, options.getMaximumPeerQueueLength());
        assertEquals(-5, options.getMinimumPeerUploadSpeed());
        assertEquals(-6, options.getFileLimit());
        assertFalse(options.isRemoveSingleCharacterSearchTerms());
        assertSame(responseFilter, options.getResponseFilter());
        assertSame(fileFilter, options.getFileFilter());
        assertSame(stateChanged, options.getStateChanged());
        assertSame(responseReceived, options.getResponseReceived());
    }

    @Test
    void usesSourceDefaults() {
        SearchOptions options = new SearchOptions();

        assertEquals(15_000, options.getSearchTimeout());
        assertEquals(250, options.getResponseLimit());
        assertTrue(options.isFilterResponses());
        assertEquals(1, options.getMinimumResponseFileCount());
        assertEquals(Integer.MAX_VALUE, options.getMaximumPeerQueueLength());
        assertEquals(0, options.getMinimumPeerUploadSpeed());
        assertEquals(25_000, options.getFileLimit());
        assertTrue(options.isRemoveSingleCharacterSearchTerms());
        assertNull(options.getResponseFilter());
        assertNull(options.getFileFilter());
        assertNull(options.getStateChanged());
        assertNull(options.getResponseReceived());
    }

    @Test
    void optionalOverloadsPreserveEveryTrailingDefault() {
        assertEquals(250, new SearchOptions(1).getResponseLimit());
        assertTrue(new SearchOptions(1, 2).isFilterResponses());
        assertEquals(1, new SearchOptions(1, 2, false).getMinimumResponseFileCount());
        assertEquals(Integer.MAX_VALUE, new SearchOptions(1, 2, false, 3).getMaximumPeerQueueLength());
        assertEquals(0, new SearchOptions(1, 2, false, 3, 4).getMinimumPeerUploadSpeed());
        assertEquals(25_000, new SearchOptions(1, 2, false, 3, 4, 5).getFileLimit());
        assertTrue(new SearchOptions(1, 2, false, 3, 4, 5, 6).isRemoveSingleCharacterSearchTerms());
        assertNull(new SearchOptions(1, 2, false, 3, 4, 5, 6, false).getResponseFilter());
    }

    @Test
    void callbacksAndNamedTupleRecordsAreUsable() {
        AtomicReference<SearchStateChange> state = new AtomicReference<>();
        AtomicReference<SearchResponseReceived> received = new AtomicReference<>();
        SearchOptions options = new SearchOptions(
                1,
                2,
                true,
                3,
                4,
                5,
                6,
                true,
                response -> response.getQueueLength() == 7,
                file -> "x".equals(file.getFilename()),
                state::set,
                received::set);
        Search search = new Search(null, null, 9, SearchStates.COMPLETED, 0, 0, 0);
        SearchResponse response = new SearchResponse("u", 8, true, 1, 7, List.of());
        File file = new File(1, "x", 2, "ext");

        options.getStateChanged().onStateChanged(new SearchStateChange(SearchStates.IN_PROGRESS, search));
        options.getResponseReceived().onResponseReceived(new SearchResponseReceived(search, response));

        assertTrue(options.getResponseFilter().test(response));
        assertTrue(options.getFileFilter().test(file));
        assertEquals(new SearchStateChange(SearchStates.IN_PROGRESS, search), state.get());
        assertEquals(new SearchResponseReceived(search, response), received.get());
    }
}
