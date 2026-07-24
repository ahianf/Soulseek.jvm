// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.search;

import dev.slsk.CacheLookupResult;
import dev.slsk.CancellationToken;
import dev.slsk.ISearchResponseCache;
import dev.slsk.RawSearchResponse;
import dev.slsk.SearchQuery;
import dev.slsk.SearchResponse;
import dev.slsk.SearchResponseCacheRecord;
import dev.slsk.diagnostics.DiagnosticEventArgs;
import dev.slsk.diagnostics.DiagnosticEventListener;
import dev.slsk.diagnostics.DiagnosticFactory;
import dev.slsk.diagnostics.IDiagnosticFactory;
import dev.slsk.eventargs.SearchRequestEventArgs;
import dev.slsk.eventargs.SearchRequestResponseEventArgs;
import dev.slsk.network.IMessageConnection;
import dev.slsk.options.SearchResponseResolver;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;

/** Responds to incoming search requests. */
public final class SearchResponder implements ISearchResponder {
    private final SearchResponderClient client;
    private final IDiagnosticFactory diagnostic;
    private final CopyOnWriteArrayList<DiagnosticEventListener> diagnosticListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<SearchResponderEventListener<SearchRequestEventArgs>> requestListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<SearchResponderEventListener<SearchRequestResponseEventArgs>>
            responseDeliveredListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<SearchResponderEventListener<SearchRequestResponseEventArgs>>
            responseFailedListeners = new CopyOnWriteArrayList<>();

    /** Creates a responder with its default diagnostic factory. */
    public SearchResponder(SearchResponderClient client) {
        this(client, null);
    }

    /** Creates a responder. */
    public SearchResponder(SearchResponderClient client, IDiagnosticFactory diagnosticFactory) {
        this.client = Objects.requireNonNull(client, "client");
        diagnostic = diagnosticFactory == null
                ? new DiagnosticFactory(client.getOptions().getMinimumDiagnosticLevel(), this::raiseDiagnostic)
                : diagnosticFactory;
    }

    @Override
    public void addDiagnosticGeneratedListener(DiagnosticEventListener listener) {
        diagnosticListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeDiagnosticGeneratedListener(DiagnosticEventListener listener) {
        diagnosticListeners.remove(listener);
    }

    @Override
    public void addRequestReceivedListener(SearchResponderEventListener<SearchRequestEventArgs> listener) {
        requestListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeRequestReceivedListener(SearchResponderEventListener<SearchRequestEventArgs> listener) {
        requestListeners.remove(listener);
    }

    @Override
    public void addResponseDeliveredListener(SearchResponderEventListener<SearchRequestResponseEventArgs> listener) {
        responseDeliveredListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeResponseDeliveredListener(SearchResponderEventListener<SearchRequestResponseEventArgs> listener) {
        responseDeliveredListeners.remove(listener);
    }

    @Override
    public void addResponseDeliveryFailedListener(
            SearchResponderEventListener<SearchRequestResponseEventArgs> listener) {
        responseFailedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeResponseDeliveryFailedListener(
            SearchResponderEventListener<SearchRequestResponseEventArgs> listener) {
        responseFailedListeners.remove(listener);
    }

    @Override
    public boolean tryDiscard(int responseToken) {
        ISearchResponseCache cache = client.getOptions().getSearchResponseCache();
        if (cache == null) {
            return false;
        }
        try {
            CacheLookupResult<SearchResponseCacheRecord> result = cache.tryRemove(responseToken);
            if (result.found()) {
                SearchResponseCacheRecord record = result.value();
                diagnostic.debug("Discarded cached search response " + responseToken
                        + " to " + record.username() + " for query '"
                        + record.query() + "' with token " + record.token());
                raiseResponseFailed(record);
                return true;
            }
        } catch (Throwable failure) {
            diagnostic.warning(
                    "Error removing cached search response " + responseToken + ": " + message(failure), failure);
        }
        return false;
    }

    @Override
    public CompletableFuture<Boolean> tryRespondAsync(String username, int token, String query) {
        try {
            raiseRequestReceived(new SearchRequestEventArgs(username, token, query));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }

        SearchResponseResolver resolver = client.getOptions().getSearchResponseResolver();
        if (resolver == null) {
            return CompletableFuture.completedFuture(false);
        }

        CompletableFuture<SearchResponse> resolution;
        try {
            resolution = Objects.requireNonNull(
                    resolver.resolve(username, token, SearchQuery.fromText(query)),
                    "searchResponseResolver returned null");
        } catch (Throwable failure) {
            warnResolution(username, token, query, failure);
            return CompletableFuture.completedFuture(false);
        }

        return resolution
                .handle((response, failure) -> {
                    if (failure != null) {
                        warnResolution(username, token, query, unwrap(failure));
                        return null;
                    }
                    return response;
                })
                .thenCompose(response -> {
                    if (response == null || response.getFileCount() + response.getLockedFileCount() <= 0) {
                        return CompletableFuture.completedFuture(false);
                    }
                    return deliverResolvedResponse(username, token, query, response);
                });
    }

    @Override
    public CompletableFuture<Boolean> tryRespondAsync(int responseToken) {
        ISearchResponseCache cache = client.getOptions().getSearchResponseCache();
        if (cache == null) {
            return CompletableFuture.completedFuture(false);
        }

        CacheLookupResult<SearchResponseCacheRecord> lookup;
        try {
            lookup = cache.tryRemove(responseToken);
        } catch (Throwable failure) {
            diagnostic.warning(
                    "Error retrieving cached search response " + responseToken + ": " + message(failure), failure);
            return CompletableFuture.completedFuture(false);
        }
        if (!lookup.found()) {
            return CompletableFuture.completedFuture(false);
        }

        SearchResponseCacheRecord record = lookup.value();
        CompletableFuture<IMessageConnection> connectionFuture;
        try {
            connectionFuture = client.getPeerConnectionManager().getCachedMessageConnectionAsync(record.username());
        } catch (Throwable failure) {
            connectionFuture = CompletableFuture.failedFuture(failure);
        }

        return connectionFuture
                .thenCompose(connection -> connection
                        .writeAsync(record.searchResponse().toByteArray())
                        .thenApply(ignored -> {
                            diagnostic.debug("Sent cached response " + responseToken
                                    + " containing "
                                    + totalFiles(record.searchResponse())
                                    + " files to " + record.username()
                                    + " for query '" + record.query()
                                    + "' with token " + record.token());
                            raiseResponseDelivered(record);
                            return true;
                        }))
                .handle((delivered, failure) -> {
                    if (failure == null) {
                        return delivered;
                    }
                    Throwable cause = unwrap(failure);
                    diagnostic.debug(
                            "Failed to send cached search response " + responseToken
                                    + " to " + record.username() + " for query '"
                                    + record.query() + "' with token " + record.token()
                                    + ": " + message(cause),
                            cause);
                    raiseResponseFailed(record);
                    return false;
                });
    }

    IDiagnosticFactory getDiagnostic() {
        return diagnostic;
    }

    private CompletableFuture<Boolean> deliverResolvedResponse(
            String username, int token, String query, SearchResponse response) {
        diagnostic.debug("Resolved " + response.getFileCount() + " files for query '" + query + "' with token " + token
                + " from " + username);

        CompletableFuture<InetSocketAddress> endpointFuture;
        try {
            endpointFuture = client.getUserEndPointAsync(username, CancellationToken.none());
        } catch (Throwable failure) {
            endpointFuture = CompletableFuture.failedFuture(failure);
        }

        return endpointFuture
                .thenCompose(endpoint -> {
                    int responseToken = client.getNextToken();
                    CompletableFuture<IMessageConnection> connectionFuture;
                    try {
                        connectionFuture = client.getPeerConnectionManager()
                                .getOrAddMessageConnectionAsync(
                                        username, endpoint, responseToken, CancellationToken.none());
                    } catch (Throwable failure) {
                        connectionFuture = CompletableFuture.failedFuture(failure);
                    }

                    return connectionFuture
                            .handle((connection, failure) -> {
                                if (failure != null) {
                                    cacheUndelivered(responseToken, username, token, query, response);
                                    throw new CompletionException(unwrap(failure));
                                }
                                return connection;
                            })
                            .thenCompose(connection -> writeResponse(connection, response));
                })
                .thenApply(ignored -> {
                    diagnostic.debug("Sent response containing " + totalFiles(response)
                            + " files to " + username + " for query '" + query
                            + "' with token " + token);
                    raiseResponseDelivered(new SearchResponseCacheRecord(username, token, query, response));
                    return true;
                })
                .handle((delivered, failure) -> {
                    if (failure == null) {
                        return delivered;
                    }
                    Throwable cause = unwrap(failure);
                    diagnostic.debug(
                            "Failed to send search response to " + username
                                    + " for query '" + query + "' with token " + token
                                    + ": " + message(cause),
                            cause);
                    return false;
                });
    }

    private CompletableFuture<Void> writeResponse(IMessageConnection connection, SearchResponse response) {
        if (response instanceof RawSearchResponse raw) {
            return connection.writeAsync(raw.getLength(), raw.getStream()).thenRun(() -> {
                try {
                    raw.getStream().close();
                } catch (Throwable ignored) {
                    // Source intentionally ignores stream-close failures.
                }
            });
        }
        return connection.writeAsync(response.toByteArray());
    }

    private void cacheUndelivered(
            int responseToken, String username, int token, String query, SearchResponse response) {
        ISearchResponseCache cache = client.getOptions().getSearchResponseCache();
        if (cache == null) {
            return;
        }
        try {
            cache.addOrUpdate(responseToken, new SearchResponseCacheRecord(username, token, query, response));
            diagnostic.debug("Failed to connect to " + username
                    + " with solicitation token " + responseToken
                    + " to deliver search results for query '" + query
                    + "' with token " + token
                    + ".  Cached response for potential delayed delivery.");
        } catch (Throwable failure) {
            diagnostic.warning(
                    "Error caching undelivered search response "
                            + responseToken + " for query '" + query
                            + "' requested by " + username + " with token "
                            + token + ": " + message(failure),
                    failure);
        }
    }

    private void warnResolution(String username, int token, String query, Throwable failure) {
        diagnostic.warning(
                "Error resolving search response for query '" + query
                        + "' requested by " + username + " with token " + token
                        + ": " + message(failure),
                failure);
    }

    private void raiseDiagnostic(DiagnosticEventArgs args) {
        diagnosticListeners.forEach(listener -> listener.handle(this, args));
    }

    private void raiseRequestReceived(SearchRequestEventArgs args) {
        requestListeners.forEach(listener -> listener.handle(this, args));
    }

    private void raiseResponseDelivered(SearchResponseCacheRecord record) {
        SearchRequestResponseEventArgs args = toEventArgs(record);
        responseDeliveredListeners.forEach(listener -> listener.handle(this, args));
    }

    private void raiseResponseFailed(SearchResponseCacheRecord record) {
        SearchRequestResponseEventArgs args = toEventArgs(record);
        responseFailedListeners.forEach(listener -> listener.handle(this, args));
    }

    private static SearchRequestResponseEventArgs toEventArgs(SearchResponseCacheRecord record) {
        return new SearchRequestResponseEventArgs(
                record.username(), record.token(), record.query(), record.searchResponse());
    }

    private static int totalFiles(SearchResponse response) {
        return response.getFileCount() + response.getLockedFileCount();
    }

    private static String message(Throwable failure) {
        return failure.getMessage() == null ? "" : failure.getMessage();
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
