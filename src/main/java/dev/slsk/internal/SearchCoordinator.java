// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static dev.slsk.internal.ClientSupport.acquirePermit;
import static dev.slsk.internal.ClientSupport.failureMessage;
import static dev.slsk.internal.ClientSupport.unwrap;

import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.DuplicateTokenException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.internal.events.SearchResponseReceivedEvent;
import dev.slsk.internal.events.SearchStateChangedEvent;
import dev.slsk.internal.messaging.messages.RoomSearchRequest;
import dev.slsk.internal.messaging.messages.UserSearchRequest;
import dev.slsk.internal.messaging.messages.WishlistSearchRequest;
import dev.slsk.internal.options.SearchOptions;
import dev.slsk.internal.options.SearchResponseReceived;
import dev.slsk.internal.options.SearchStateChange;
import dev.slsk.internal.search.SearchInternal;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Search lifecycle: issuing a query, routing responses back to the caller and
 * settling the search when it completes, times out or is cancelled.
 *
 * <p>The search registry itself stays on the client, because incoming
 * distributed and peer messages are dispatched against it from the message
 * handlers. This owns the caller-facing half.
 */
final class SearchCoordinator {

    private final ClientContext context;

    /** Caps concurrent searches; the limit is a search concern, so it lives here. */
    private final java.util.concurrent.Semaphore searchSemaphore;

    SearchCoordinator(ClientContext context) {
        this.context = java.util.Objects.requireNonNull(context, "context");
        this.searchSemaphore =
                new java.util.concurrent.Semaphore(context.getClientOptions().getMaximumConcurrentSearches());
    }

    /**
     * Searches the network and collects accepted responses.
     *
     * @param query the search query
     * @return the completed search and collected responses
     */
    CompletableFuture<SearchResult> search(SearchQuery query) {
        return search(query, null, null, null, CancellationSignal.none());
    }
    /**
     * Searches the network and collects accepted responses.
     *
     * @param query the search query
     * @param cancellationSignal the cancellation signal
     * @return the completed search and collected responses
     */
    CompletableFuture<SearchResult> search(SearchQuery query, CancellationSignal cancellationSignal) {
        return search(query, null, null, null, cancellationSignal);
    }
    /**
     * Searches the selected scope and collects accepted responses.
     *
     * @param query the search query
     * @param scope the search scope
     * @return the completed search and collected responses
     */
    CompletableFuture<SearchResult> search(SearchQuery query, SearchScope scope) {
        return search(query, scope, null, null, CancellationSignal.none());
    }
    /**
     * Searches the selected scope with a specific token.
     *
     * @param query the search query
     * @param scope the search scope
     * @param token the unique token
     * @return the completed search and collected responses
     */
    CompletableFuture<SearchResult> search(SearchQuery query, SearchScope scope, Integer token) {
        return search(query, scope, token, null, CancellationSignal.none());
    }
    /**
     * Searches the selected scope using the supplied context.getClientOptions().
     *
     * @param query the search query
     * @param scope the search scope
     * @param token the unique token
     * @param searchOptions the search options
     * @return the completed search and collected responses
     */
    CompletableFuture<SearchResult> search(
            SearchQuery query, SearchScope scope, Integer token, SearchOptions searchOptions) {
        return search(query, scope, token, searchOptions, CancellationSignal.none());
    }
    /**
     * Searches the selected scope and collects accepted responses.
     *
     * @param query the search query
     * @param scope the search scope, or {@code null} for the network
     * @param token the unique token, or {@code null} to generate one
     * @param searchOptions the search options, or {@code null} for defaults
     * @param cancellationSignal the cancellation signal
     * @return the completed search and collected responses
     */
    CompletableFuture<SearchResult> search(
            SearchQuery query,
            SearchScope scope,
            Integer token,
            SearchOptions searchOptions,
            CancellationSignal cancellationSignal) {
        SearchInvocation invocation = validateSearch(query, scope, token, searchOptions);
        List<SearchResponse> responses = Collections.synchronizedList(new ArrayList<>());
        return searchToCallbackAsync(invocation, responses::add, context.defaultToken(cancellationSignal))
                .thenApply(search -> {
                    synchronized (responses) {
                        return new SearchResult(search, responses);
                    }
                });
    }
    /**
     * Searches the network and invokes a handler for each accepted response.
     *
     * @param query the search query
     * @param responseHandler the response handler
     * @return the completed search
     */
    CompletableFuture<Search> search(SearchQuery query, Consumer<SearchResponse> responseHandler) {
        return search(query, responseHandler, null, null, null, CancellationSignal.none());
    }
    /**
     * Searches the network and invokes a handler for each accepted response.
     *
     * @param query the search query
     * @param responseHandler the response handler
     * @param cancellationSignal the cancellation signal
     * @return the completed search
     */
    CompletableFuture<Search> search(
            SearchQuery query, Consumer<SearchResponse> responseHandler, CancellationSignal cancellationSignal) {
        return search(query, responseHandler, null, null, null, cancellationSignal);
    }
    /**
     * Searches the selected scope and invokes a response handler.
     *
     * @param query the search query
     * @param responseHandler the response handler
     * @param scope the search scope
     * @return the completed search
     */
    CompletableFuture<Search> search(SearchQuery query, Consumer<SearchResponse> responseHandler, SearchScope scope) {
        return search(query, responseHandler, scope, null, null, CancellationSignal.none());
    }
    /**
     * Searches the selected scope with a specific token.
     *
     * @param query the search query
     * @param responseHandler the response handler
     * @param scope the search scope
     * @param token the unique token
     * @return the completed search
     */
    CompletableFuture<Search> search(
            SearchQuery query, Consumer<SearchResponse> responseHandler, SearchScope scope, Integer token) {
        return search(query, responseHandler, scope, token, null, CancellationSignal.none());
    }
    /**
     * Searches the selected scope using the supplied context.getClientOptions().
     *
     * @param query the search query
     * @param responseHandler the response handler
     * @param scope the search scope
     * @param token the unique token
     * @param searchOptions the search options
     * @return the completed search
     */
    CompletableFuture<Search> search(
            SearchQuery query,
            Consumer<SearchResponse> responseHandler,
            SearchScope scope,
            Integer token,
            SearchOptions searchOptions) {
        return search(query, responseHandler, scope, token, searchOptions, CancellationSignal.none());
    }
    /**
     * Searches the selected scope and invokes a handler for each accepted
     * response.
     *
     * @param query the search query
     * @param responseHandler the response handler
     * @param scope the search scope, or {@code null} for the network
     * @param token the unique token, or {@code null} to generate one
     * @param searchOptions the search options, or {@code null} for defaults
     * @param cancellationSignal the cancellation signal
     * @return the completed search
     */
    CompletableFuture<Search> search(
            SearchQuery query,
            Consumer<SearchResponse> responseHandler,
            SearchScope scope,
            Integer token,
            SearchOptions searchOptions,
            CancellationSignal cancellationSignal) {
        SearchQuery validatedQuery = validateSearchQuery(query);
        Objects.requireNonNull(responseHandler, "responseHandler");
        SearchInvocation invocation = validateSearch(validatedQuery, scope, token, searchOptions);
        return searchToCallbackAsync(invocation, responseHandler, context.defaultToken(cancellationSignal));
    }

    SearchInvocation validateSearch(
            SearchQuery initialQuery, SearchScope initialScope, Integer initialToken, SearchOptions initialOptions) {
        SearchQuery query = validateSearchQuery(initialQuery);
        context.requireLoggedIn("perform a search");

        int token = initialToken == null ? context.getTokenFactory().nextToken() : initialToken;
        if (context.getSearchRegistry().containsKey(token)) {
            throw new DuplicateTokenException("An active search with token " + token + " is already in progress");
        }

        SearchScope scope = initialScope == null ? SearchScope.getNetwork() : initialScope;
        SearchOptions searchOptions = initialOptions == null ? new SearchOptions() : initialOptions;
        if (searchOptions.isRemoveSingleCharacterSearchTerms()) {
            query = new SearchQuery(
                    query.getTerms().stream().filter(term -> term.length() > 1).toList(), query.getExclusions());
        }
        if (query.getTerms().isEmpty()) {
            throw new IllegalArgumentException(
                    "Search query must contain at least one non-exclusion " + "term with length greater than 1");
        }
        return new SearchInvocation(query, scope, token, searchOptions);
    }

    static SearchQuery validateSearchQuery(SearchQuery initialQuery) {
        SearchQuery query = Objects.requireNonNull(initialQuery, "query");
        if (dev.slsk.internal.common.CommonUtils.isNullOrWhiteSpace(query.getSearchText())) {
            throw new IllegalArgumentException("Search text must not be null, empty, or whitespace");
        }
        if (query.getTerms().isEmpty()) {
            throw new IllegalArgumentException("Search query must contain at least one " + "non-exclusion term");
        }
        return query;
    }

    record SearchInvocation(SearchQuery query, SearchScope scope, int token, SearchOptions options) {}

    CompletableFuture<Search> searchToCallbackAsync(
            SearchInvocation invocation,
            Consumer<SearchResponse> responseHandler,
            CancellationSignal cancellationSignal) {
        SearchInternal search = new SearchInternal(
                invocation.query(),
                invocation.scope(),
                invocation.token(),
                invocation.options(),
                context.getScheduler());
        SearchState[] previousState = {SearchState.NONE};
        Consumer<SearchState> updateState = newState -> {
            search.setState(newState);
            Search snapshot = search.toSearch();
            SearchStateChangedEvent eventData = new SearchStateChangedEvent(previousState[0], snapshot);
            previousState[0] = newState;
            if (invocation.options().getStateChanged() != null) {
                invocation
                        .options()
                        .getStateChanged()
                        .onStateChanged(new SearchStateChange(eventData.getPreviousState(), eventData.getSearch()));
            }
            context.raiseSearchEvent(DefaultSoulseekClient.Event.SEARCH_STATE_CHANGED, eventData);
        };

        CompletableFuture<Search> operation;
        try {
            context.getSearchRegistry().putIfAbsent(search.getToken(), search);
            updateState.accept(SearchState.REQUESTED);
            context.getDiagnostic()
                    .debug("Attempting to acquire search semaphore for search '"
                            + invocation.query().getSearchText() + "' ("
                            + searchSemaphore.availablePermits()
                            + " available)");
            updateState.accept(SearchState.QUEUED);
            operation = acquireSearchPermit(cancellationSignal).thenCompose(ignored -> {
                context.getDiagnostic()
                        .debug("Acquired search semaphore for search '"
                                + invocation.query().getSearchText() + "'");
                CompletableFuture<Search> activeSearch;
                try {
                    byte[] message = buildSearchMessage(invocation.scope(), search);
                    search.setResponseReceived(response -> {
                        responseHandler.accept(response);
                        SearchResponseReceivedEvent eventData =
                                new SearchResponseReceivedEvent(response, search.toSearch());
                        if (invocation.options().getResponseReceived() != null) {
                            invocation
                                    .options()
                                    .getResponseReceived()
                                    .onResponseReceived(
                                            new SearchResponseReceived(eventData.getSearch(), eventData.getResponse()));
                        }
                        context.raiseSearchEvent(DefaultSoulseekClient.Event.SEARCH_RESPONSE_RECEIVED, eventData);
                    });
                    activeSearch = context.writeBytesToServer(message, cancellationSignal)
                            .thenRun(() -> updateState.accept(SearchState.IN_PROGRESS))
                            .thenCompose(ignoredWrite -> search.waitForCompletion(cancellationSignal))
                            .thenApply(ignoredCompletion -> {
                                updateState.accept(SearchState.COMPLETED.or(search.getState()));
                                context.getDiagnostic()
                                        .debug("Search for '"
                                                + invocation.query().getSearchText()
                                                + "' completed: "
                                                + search.getState());
                                return search.toSearch();
                            });
                } catch (Throwable failure) {
                    activeSearch = CompletableFuture.failedFuture(failure);
                }
                return activeSearch.whenComplete((result, failure) -> {
                    searchSemaphore.release();
                    context.getDiagnostic()
                            .debug("Released search semaphore for search '"
                                    + invocation.query().getSearchText()
                                    + "' ("
                                    + searchSemaphore.availablePermits()
                                    + " available)");
                });
            });
        } catch (Throwable failure) {
            operation = CompletableFuture.failedFuture(failure);
        }

        return operation
                .handle((result, failure) -> {
                    if (failure == null) {
                        return result;
                    }
                    Throwable cause = unwrap(failure);
                    if (cause instanceof CancellationException) {
                        search.complete(SearchState.CANCELLED);
                        updateState.accept(SearchState.COMPLETED.or(SearchState.CANCELLED));
                        throw new CompletionException(cause);
                    }
                    search.complete(SearchState.ERRORED);
                    updateState.accept(SearchState.COMPLETED.or(SearchState.ERRORED));
                    if (cause instanceof TimeoutException) {
                        throw new CompletionException(cause);
                    }
                    throw new CompletionException(new SoulseekClientException(
                            "Failed to search for "
                                    + invocation.query().getSearchText()
                                    + " (" + invocation.token() + "): "
                                    + failureMessage(cause),
                            cause));
                })
                .whenComplete((result, failure) -> {
                    context.getSearchRegistry().remove(search.getToken(), search);
                    search.close();
                });
    }

    CompletableFuture<Void> acquireSearchPermit(CancellationSignal cancellationSignal) {
        return acquirePermit(searchSemaphore, cancellationSignal);
    }

    static byte[] buildSearchMessage(SearchScope scope, SearchInternal search) {
        String text = search.getQuery().getSearchText();
        return switch (scope.getType()) {
            case ROOM ->
                new RoomSearchRequest(scope.getSubjects().iterator().next(), text, search.getToken()).toByteArray();
            case USER -> {
                ByteArrayOutputStream messages = new ByteArrayOutputStream();
                for (String subject : scope.getSubjects()) {
                    messages.writeBytes(new UserSearchRequest(subject, text, search.getToken()).toByteArray());
                }
                yield messages.toByteArray();
            }
            case WISHLIST -> new WishlistSearchRequest(text, search.getToken()).toByteArray();
            case NETWORK ->
                new dev.slsk.internal.messaging.messages.SearchRequest(text, search.getToken()).toByteArray();
        };
    }
}
