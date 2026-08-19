// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.exceptions.DuplicateTokenException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.common.CommonUtils;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.Permits;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.events.SearchResponseReceivedEvent;
import dev.slsk.internal.events.SearchStateChangedEvent;
import dev.slsk.internal.messaging.messages.RoomSearchRequest;
import dev.slsk.internal.messaging.messages.UserSearchRequest;
import dev.slsk.internal.messaging.messages.WishlistSearchRequest;
import dev.slsk.internal.options.SearchOptions;
import dev.slsk.internal.options.SearchResponseReceived;
import dev.slsk.internal.options.SearchStateChange;
import dev.slsk.internal.search.Search;
import dev.slsk.internal.search.SearchInternal;
import dev.slsk.internal.search.SearchQuery;
import dev.slsk.internal.search.SearchResponse;
import dev.slsk.internal.search.SearchResult;
import dev.slsk.internal.search.SearchScope;
import dev.slsk.internal.search.SearchState;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Searches: issuing a query, the registry of the ones in flight, routing
 * responses back to the caller, and settling each one when it completes, times
 * out or is cancelled.
 *
 * <p>The registry used to live on the engine, on the grounds that incoming
 * distributed and peer messages are dispatched against it from the message
 * handlers. That is a reason to give the handlers a way in, not a reason to
 * keep a search's state somewhere other than searches: everything that decides
 * what a search is doing is here, so the map of what is running belongs here
 * too.
 */
final class SearchDomain {

    private final SoulseekEngine context;
    private final ServerLink server;

    /** Caps concurrent searches; the limit is a search concern, so it lives here. */
    private final java.util.concurrent.Semaphore searchSemaphore;

    /**
     * The searches in flight, by the token the network answers on.
     *
     * <p>Volatile and replaceable because a test substitutes it wholesale.
     */
    private volatile java.util.Map<Integer, SearchInternal> searches = new java.util.concurrent.ConcurrentHashMap<>();

    SearchDomain(SoulseekEngine context, ServerLink server) {
        this.context = java.util.Objects.requireNonNull(context, "context");
        this.server = java.util.Objects.requireNonNull(server, "server");
        this.searchSemaphore =
                new java.util.concurrent.Semaphore(context.getClientOptions().getMaximumConcurrentSearches());
    }

    /**
     * Returns the searches in flight, by token.
     *
     * @return the live registry
     */
    java.util.Map<Integer, SearchInternal> registry() {
        return searches;
    }

    /**
     * Replaces the registry. For tests that need to observe one they own.
     *
     * @param value the registry
     */
    void registry(java.util.Map<Integer, SearchInternal> value) {
        searches = value;
    }

    /**
     * Cancels every search in flight and forgets them.
     *
     * <p>What a disconnect does to searches: none of them can be answered any
     * more, and each has callers waiting on a terminal state.
     */
    void cancelAll() {
        for (SearchInternal search : new java.util.ArrayList<>(searches.values())) {
            search.cancel();
            search.close();
        }
        searches.clear();
    }

    /**
     * Searches the network and collects accepted responses.
     *
     * @param query the search query
     * @return the completed search and collected responses
     */
    /**
     * Searches as the request describes and collects accepted responses.
     *
     * <p>The request object is how a caller states a search; unpacking it into
     * five positional arguments was a blocking wrapper's job on the client, and
     * the client is gone.
     *
     * @param request the search to perform
     * @return the completed search and collected responses
     */
    SearchResult search(dev.slsk.internal.search.SearchRequest request) {
        // Qualified: this file also talks to the wire message of the same name.
        java.util.Objects.requireNonNull(request, "request");
        return search(
                request.getQuery(),
                request.getScope(),
                request.getToken(),
                request.getOptions(),
                request.getCancellationSignal());
    }

    /**
     * Searches as the request describes, streaming each accepted response.
     *
     * @param request the search to perform
     * @param responseHandler receives each accepted response
     * @return the completed search
     */
    Search search(dev.slsk.internal.search.SearchRequest request, Consumer<SearchResponse> responseHandler) {
        java.util.Objects.requireNonNull(request, "request");
        java.util.Objects.requireNonNull(responseHandler, "responseHandler");
        return search(
                request.getQuery(),
                responseHandler,
                request.getScope(),
                request.getToken(),
                request.getOptions(),
                request.getCancellationSignal());
    }

    SearchResult search(
            SearchQuery query,
            SearchScope scope,
            Integer token,
            SearchOptions searchOptions,
            CancellationSignal cancellationSignal) {
        SearchInvocation invocation = validateSearch(query, scope, token, searchOptions);
        List<SearchResponse> responses = Collections.synchronizedList(new ArrayList<>());
        Search search = searchToCallback(invocation, responses::add, CommonUtils.token(cancellationSignal));
        synchronized (responses) {
            return new SearchResult(search, responses);
        }
    }
    /**
     * Searches the network and invokes a handler for each accepted response.
     *
     * @param query the search query
     * @param responseHandler the response handler
     * @return the completed search
     */
    Search search(
            SearchQuery query,
            Consumer<SearchResponse> responseHandler,
            SearchScope scope,
            Integer token,
            SearchOptions searchOptions,
            CancellationSignal cancellationSignal) {
        SearchQuery validatedQuery = validateSearchQuery(query);
        Objects.requireNonNull(responseHandler, "responseHandler");
        SearchInvocation invocation = validateSearch(validatedQuery, scope, token, searchOptions);
        return searchToCallback(invocation, responseHandler, CommonUtils.token(cancellationSignal));
    }

    SearchInvocation validateSearch(
            SearchQuery initialQuery, SearchScope initialScope, Integer initialToken, SearchOptions initialOptions) {
        SearchQuery query = validateSearchQuery(initialQuery);
        server.requireLoggedIn("perform a search");

        int token = initialToken == null ? context.getTokenFactory().nextToken() : initialToken;
        if (searches.containsKey(token)) {
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

    Search searchToCallback(
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
            context.raiseEvent(Kind.SEARCH_STATE_CHANGED, eventData);
        };

        try {
            // The validate-time containsKey check races concurrent callers;
            // this insertion is the authoritative claim on the token.
            if (searches.putIfAbsent(search.getToken(), search) != null) {
                throw new DuplicateTokenException(
                        "An active search with token " + search.getToken() + " is already in progress");
            }
            updateState.accept(SearchState.REQUESTED);
            context.getDiagnostic()
                    .debug("Attempting to acquire search semaphore for search '"
                            + invocation.query().getSearchText() + "' ("
                            + searchSemaphore.availablePermits()
                            + " available)");
            updateState.accept(SearchState.QUEUED);
            acquireSearchPermit(cancellationSignal);
            context.getDiagnostic()
                    .debug("Acquired search semaphore for search '"
                            + invocation.query().getSearchText() + "'");
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
                    context.raiseEvent(Kind.SEARCH_RESPONSE_RECEIVED, eventData);
                });
                server.writeBytes(message, cancellationSignal);
                updateState.accept(SearchState.IN_PROGRESS);
                search.waitForCompletion(cancellationSignal);
                updateState.accept(SearchState.COMPLETED.or(search.getState()));
                context.getDiagnostic()
                        .debug("Search for '"
                                + invocation.query().getSearchText()
                                + "' completed: "
                                + search.getState());
                return search.toSearch();
            } finally {
                searchSemaphore.release();
                context.getDiagnostic()
                        .debug("Released search semaphore for search '"
                                + invocation.query().getSearchText()
                                + "' ("
                                + searchSemaphore.availablePermits()
                                + " available)");
            }
        } catch (Throwable cause) {
            if (cause instanceof CancellationException) {
                search.complete(SearchState.CANCELLED);
                updateState.accept(SearchState.COMPLETED.or(SearchState.CANCELLED));
                throw Failures.surface(cause);
            }
            search.complete(SearchState.ERRORED);
            updateState.accept(SearchState.COMPLETED.or(SearchState.ERRORED));
            if (cause instanceof TimeoutException || cause instanceof DuplicateTokenException) {
                throw Failures.surface(cause);
            }
            throw new SoulseekClientException(
                    "Failed to search for "
                            + invocation.query().getSearchText()
                            + " (" + invocation.token() + "): "
                            + Failures.message(cause),
                    cause);
        } finally {
            searches.remove(search.getToken(), search);
            search.close();
        }
    }

    void acquireSearchPermit(CancellationSignal cancellationSignal) throws InterruptedException {
        Permits.acquire(searchSemaphore, cancellationSignal);
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
