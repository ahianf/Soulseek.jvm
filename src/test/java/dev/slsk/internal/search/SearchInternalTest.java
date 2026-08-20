// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.concurrent.CancellationController;
import dev.slsk.internal.options.SearchOptions;
import dev.slsk.internal.share.File;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class SearchInternalTest {
    private static final File FILE = new File(1, "a", 2, "ext");

    @Test
    void instantiatesWithExpectedDataAndDefaults() {
        SearchQuery query = new SearchQuery("foo");
        SearchScope scope = SearchScope.getNetwork();
        SearchOptions options = new SearchOptions();
        try (SearchInternal search = new SearchInternal(query, scope, 42, options)) {
            assertSame(query, search.getQuery());
            assertSame(scope, search.getScope());
            assertSame(options, search.getOptions());
            assertEquals(42, search.getToken());
            assertEquals(SearchPhase.NONE, search.getState());
            assertEquals(0, search.getResponseCount());
            assertEquals(0, search.getFileCount());
            assertEquals(0, search.getLockedFileCount());
            assertFalse(search.isTimeoutActive());
        }

        try (SearchInternal search = new SearchInternal(query, scope, 42)) {
            assertNotNull(search.getOptions());
        }
    }

    @Test
    void closeCompleteAndCancelAreIdempotent() {
        SearchInternal search = search(42, new SearchOptions());
        search.complete(SearchTermination.TIMED_OUT);
        search.complete(SearchTermination.TIMED_OUT);
        assertEquals(SearchPhase.COMPLETED, search.getState());
        assertEquals(SearchTermination.TIMED_OUT, search.getTermination());
        search.close();
        search.close();

        SearchInternal cancelled = search(42, new SearchOptions());
        cancelled.cancel();
        cancelled.cancel();
        assertEquals(SearchPhase.COMPLETED, cancelled.getState());
        assertEquals(SearchTermination.CANCELLED, cancelled.getTermination());
        assertThrows(CancellationException.class, cancelled::waitForCompletion);
        cancelled.close();
    }

    @Test
    void criteriaRespectFilterSwitchAndEveryThreshold() {
        assertAccepted(options(1000, 250, false, 99, 0, 99), response(42, 0, 0, List.of(), List.of()));
        assertRejected(options(1000, 250, true, 1, 2, 3), response(42, 3, 1, List.of(), List.of()));
        assertRejected(options(1000, 250, true, 1, 2, 3), response(42, 2, 2, List.of(FILE), List.of()));
        assertAccepted(options(1000, 250, true, 1, 2, 3), response(42, 3, 1, List.of(FILE), List.of()));
    }

    @Test
    void ignoresWrongStateAndRejectedCustomResponse() {
        AtomicInteger received = new AtomicInteger();
        try (SearchInternal search = search(42, new SearchOptions())) {
            search.setResponseReceived(response -> received.incrementAndGet());
            search.complete(SearchTermination.TIMED_OUT);
            search.tryAddResponse(response(42, 1, 0, List.of(FILE), List.of()));
            assertEquals(0, received.get());
        }

        SearchOptions options = options(1000)
                .minimumResponseFileCount(0)
                .responseFilter(response -> false)
                .build();
        assertRejected(options, response(42, 1, 0, List.of(FILE), List.of()));
    }

    @Test
    void rejectsMismatchedTokenBeforeClosedCheck() {
        SearchInternal search = search(42, new SearchOptions());
        search.close();

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> search.tryAddResponse(response(24, 1, 0, List.of(FILE), List.of())));

        assertTrue(failure.getMessage().contains("with token 42 received response with search token 24"));
    }

    @Test
    void filtersUnlockedAndLockedFilesBeforeAccepting() {
        File keep = new File(1, "keep", 1, "x");
        File remove = new File(1, "remove", 1, "x");
        SearchOptions options = options(1000).fileFilter(file -> file == keep).build();
        AtomicReference<SearchResponse> accepted = new AtomicReference<>();

        try (SearchInternal search = search(42, options)) {
            search.setState(SearchPhase.IN_PROGRESS);
            search.setResponseReceived(accepted::set);
            search.tryAddResponse(response(42, 1, 0, List.of(keep, remove), List.of(keep, remove)));

            assertEquals(List.of(keep), accepted.get().files());
            assertEquals(List.of(keep), accepted.get().lockedFiles());
            assertEquals(1, search.getResponseCount());
            assertEquals(1, search.getFileCount());
            assertEquals(1, search.getLockedFileCount());
        }
    }

    @Test
    void rejectsResponseWhenFileFilterDropsBelowMinimum() {
        SearchOptions options = options(1000).fileFilter(file -> false).build();
        assertRejected(options, response(42, 1, 0, List.of(), List.of(FILE)));
    }

    @Test
    void invokesRegisteredCallbacksInOrderAndResetsTimeout() {
        StringBuilder order = new StringBuilder();
        try (SearchInternal search = search(42, options(1000).build())) {
            search.setState(SearchPhase.IN_PROGRESS);
            search.addResponseReceived(response -> order.append('a'));
            search.addResponseReceived(response -> order.append('b'));
            java.util.concurrent.ScheduledFuture<?> timeout = search.timeoutTaskForTest();
            search.tryAddResponse(response(42, 1, 0, List.of(FILE), List.of()));

            assertEquals("ab", order.toString());
            assertTrue(search.isTimeoutActive());
            assertSame(timeout, search.timeoutTaskForTest(), "a response advances the deadline without rescheduling");
        }
    }

    @Test
    void returnsWithoutCallbackAfterCloseAndSurfacesCallbackFailure() {
        AtomicInteger count = new AtomicInteger();
        SearchInternal search = search(42, new SearchOptions());
        search.setState(SearchPhase.IN_PROGRESS);
        search.setResponseReceived(response -> count.incrementAndGet());
        search.close();
        search.tryAddResponse(response(42, 1, 0, List.of(FILE), List.of()));
        assertEquals(0, count.get());

        try (SearchInternal second = search(42, new SearchOptions())) {
            second.setState(SearchPhase.IN_PROGRESS);
            IllegalStateException callbackFailure = new IllegalStateException("callback failed");
            second.setResponseReceived(response -> {
                throw callbackFailure;
            });
            IllegalStateException thrown = assertThrows(
                    IllegalStateException.class,
                    () -> second.tryAddResponse(response(42, 1, 0, List.of(FILE), List.of())));
            assertSame(callbackFailure, thrown);
            assertEquals(1, second.getResponseCount());
        }
    }

    @Test
    void responseLimitWinsWhenBothLimitsAreReached() throws Exception {
        SearchOptions options = options(1000)
                .responseLimit(1)
                .filterResponses(false)
                .fileLimit(1)
                .build();
        try (SearchInternal search = search(42, options)) {
            search.setState(SearchPhase.IN_PROGRESS);
            search.tryAddResponse(response(42, 1, 0, List.of(FILE), List.of()));
            search.waitForCompletion();
            assertEquals(SearchPhase.COMPLETED, search.getState());
            assertEquals(SearchTermination.RESPONSE_LIMIT_REACHED, search.getTermination());
        }
    }

    @Test
    void fileLimitCompletesSearch() throws Exception {
        SearchOptions options = options(1000)
                .responseLimit(2)
                .filterResponses(false)
                .fileLimit(1)
                .build();
        try (SearchInternal search = search(42, options)) {
            search.setState(SearchPhase.IN_PROGRESS);
            search.tryAddResponse(response(42, 1, 0, List.of(FILE), List.of()));
            search.waitForCompletion();
            assertEquals(SearchPhase.COMPLETED, search.getState());
            assertEquals(SearchTermination.FILE_LIMIT_REACHED, search.getTermination());
        }
    }

    @Test
    void waitCompletesAndCallerCancellationDoesNotCancelSearch() {
        try (SearchInternal search = search(42, new SearchOptions())) {
            // Complete first, then wait: a blocking wait on a search nothing
            // has finished would park this thread for the search timeout.
            search.complete(SearchTermination.TIMED_OUT);
            assertDoesNotThrow(() -> search.waitForCompletion());
        }

        try (SearchInternal search = search(42, new SearchOptions());
                CancellationController source = new CancellationController()) {
            source.cancel();
            assertThrows(CancellationException.class, () -> search.waitForCompletion(source.getSignal()));
            assertEquals(SearchPhase.NONE, search.getState());
        }
    }

    @Test
    void timerStartsOnlyOnTransitionsIntoInProgressAndTimesOut() throws Exception {
        SearchOptions options = options(40).build();
        try (SearchInternal search = search(42, options)) {
            assertFalse(search.isTimeoutActive());
            search.setState(SearchPhase.REQUESTED);
            search.setState(SearchPhase.QUEUED);
            assertFalse(search.isTimeoutActive());
            search.setState(SearchPhase.IN_PROGRESS);
            assertTrue(search.isTimeoutActive());
            search.setState(SearchPhase.IN_PROGRESS);
            assertTrue(search.isTimeoutActive());
            search.waitForCompletion();
            assertEquals(SearchPhase.COMPLETED, search.getState());
            assertEquals(SearchTermination.TIMED_OUT, search.getTermination());
            assertFalse(search.isTimeoutActive());
        }
    }

    @Test
    void snapshotCopiesCurrentMutableState() {
        try (SearchInternal search = search(42, new SearchOptions())) {
            search.setState(SearchPhase.IN_PROGRESS);
            search.tryAddResponse(response(42, 1, 0, List.of(FILE), List.of(FILE)));
            Search snapshot = search.toSearch();
            assertSame(search.getQuery(), snapshot.query());
            assertSame(search.getScope(), snapshot.scope());
            assertEquals(42, snapshot.token());
            assertEquals(SearchPhase.IN_PROGRESS, snapshot.state());
            assertNull(snapshot.termination());
            assertEquals(1, snapshot.responseCount());
            assertEquals(1, snapshot.fileCount());
            assertEquals(1, snapshot.lockedFileCount());
        }
    }

    @Test
    void rejectsNonpositiveTimeoutLikeSystemTimer() {
        assertThrows(IllegalArgumentException.class, () -> search(42, options(0).build()));
    }

    private static SearchInternal search(int token, SearchOptions options) {
        return new SearchInternal(new SearchQuery("foo"), SearchScope.getNetwork(), token, options);
    }

    private static SearchOptions.Builder options(int timeoutMillis) {
        return SearchOptions.builder().searchTimeout(Duration.ofMillis(timeoutMillis));
    }

    private static SearchOptions options(
            int timeoutMillis,
            int responseLimit,
            boolean filterResponses,
            int minimumResponseFileCount,
            int maximumPeerQueueLength,
            int minimumPeerUploadSpeed) {
        return options(timeoutMillis)
                .responseLimit(responseLimit)
                .filterResponses(filterResponses)
                .minimumResponseFileCount(minimumResponseFileCount)
                .maximumPeerQueueLength(maximumPeerQueueLength)
                .minimumPeerUploadSpeed(minimumPeerUploadSpeed)
                .build();
    }

    private static SearchResponse response(
            int token, int uploadSpeed, int queueLength, List<File> files, List<File> lockedFiles) {
        return new SearchResponse("user", token, true, uploadSpeed, queueLength, files, lockedFiles);
    }

    private static void assertAccepted(SearchOptions options, SearchResponse response) {
        try (SearchInternal search = search(42, options)) {
            search.setState(SearchPhase.IN_PROGRESS);
            search.tryAddResponse(response);
            assertEquals(1, search.getResponseCount());
        }
    }

    private static void assertRejected(SearchOptions options, SearchResponse response) {
        try (SearchInternal search = search(42, options)) {
            search.setState(SearchPhase.IN_PROGRESS);
            search.tryAddResponse(response);
            assertEquals(0, search.getResponseCount());
        }
    }
}
