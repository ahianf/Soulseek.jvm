// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import dev.slsk.Subscription;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.TokenFactory;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.diagnostics.DiagnosticEvent;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.diagnostics.FilteringDiagnosticSink;
import dev.slsk.internal.events.SearchRequestEvent;
import dev.slsk.internal.events.SearchRequestResponseEvent;
import dev.slsk.internal.events.Subscriptions;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.PeerConnectionManager;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.share.Catalogs;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Responds to incoming search requests. */
public final class DefaultSearchResponder implements SearchResponder {

    /** The most files worth answering one peer's search with; shared with the direct-request path. */
    private static final int MAXIMUM_MATCHES = Catalogs.MAXIMUM_SEARCH_MATCHES;

    /** Where a peer is, so a response can be delivered to them. */
    @FunctionalInterface
    public interface Endpoints {
        /**
         * Resolves a peer's endpoint.
         *
         * @param username the peer
         * @param cancellationSignal the cancellation signal
         * @return the endpoint
         */
        java.net.InetSocketAddress resolve(String username, CancellationSignal cancellationSignal)
                throws InterruptedException;
    }

    private final Supplier<SoulseekClientOptions> options;
    private final Supplier<PeerConnectionManager> peers;
    private final TokenFactory tokens;
    private final Endpoints endpoints;
    private final Supplier<dev.slsk.spi.ShareCatalog> catalog;
    private final Supplier<String> loggedInUsername;

    /**
     * Our advertised average upload speed, in bytes per second; peers read it
     * from the response to rank us as a source. The transfer domain owns the
     * number — it is the server's average for this account as last heard.
     */
    private final java.util.function.IntSupplier advertisedUploadSpeed;

    private final DiagnosticSink diagnostic;
    private final CopyOnWriteArrayList<Consumer<? super DiagnosticEvent>> diagnosticListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super SearchRequestEvent>> requestListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super SearchRequestResponseEvent>> responseDeliveredListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super SearchRequestResponseEvent>> responseFailedListeners =
            new CopyOnWriteArrayList<>();
    private final Object evictionBinding = new Object();
    private volatile SearchResponseCache evictionBoundTo;

    /** Creates a responder with its default diagnostic factory. */
    public DefaultSearchResponder(
            Supplier<SoulseekClientOptions> options,
            Supplier<PeerConnectionManager> peers,
            TokenFactory tokens,
            Endpoints endpoints,
            Supplier<dev.slsk.spi.ShareCatalog> catalog,
            Supplier<String> loggedInUsername,
            java.util.function.IntSupplier advertisedUploadSpeed) {
        this(options, peers, tokens, endpoints, catalog, loggedInUsername, advertisedUploadSpeed, null);
    }

    /** Creates a responder. */
    public DefaultSearchResponder(
            Supplier<SoulseekClientOptions> options,
            Supplier<PeerConnectionManager> peers,
            TokenFactory tokens,
            Endpoints endpoints,
            Supplier<dev.slsk.spi.ShareCatalog> catalog,
            Supplier<String> loggedInUsername,
            java.util.function.IntSupplier advertisedUploadSpeed,
            DiagnosticSink diagnosticFactory) {
        this.options = Objects.requireNonNull(options, "options");
        this.peers = Objects.requireNonNull(peers, "peers");
        this.tokens = Objects.requireNonNull(tokens, "tokens");
        this.endpoints = Objects.requireNonNull(endpoints, "endpoints");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.loggedInUsername = Objects.requireNonNull(loggedInUsername, "loggedInUsername");
        this.advertisedUploadSpeed = Objects.requireNonNull(advertisedUploadSpeed, "advertisedUploadSpeed");
        diagnostic = diagnosticFactory == null
                ? new FilteringDiagnosticSink(options.get().minimumDiagnosticLevel(), this::publishDiagnostic)
                : DiagnosticSink.forSource(diagnosticFactory, DefaultSearchResponder.class);
    }

    @Override
    public Subscription subscribe(Consumer<? super DiagnosticEvent> listener) {
        return Subscriptions.add(diagnosticListeners, listener);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Subscription subscribe(Kind kind, Consumer<? super T> listener) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(listener, "listener");
        return switch (kind) {
            case REQUEST_RECEIVED ->
                Subscriptions.add(requestListeners, (Consumer<? super SearchRequestEvent>) listener);
            case RESPONSE_DELIVERED ->
                Subscriptions.add(responseDeliveredListeners, (Consumer<? super SearchRequestResponseEvent>) listener);
            case RESPONSE_DELIVERY_FAILED ->
                Subscriptions.add(responseFailedListeners, (Consumer<? super SearchRequestResponseEvent>) listener);
        };
    }

    @Override
    public boolean tryDiscard(int responseToken) {
        SearchResponseCache cache = options.get().searchResponseCache();
        if (cache == null) {
            return false;
        }
        try {
            Optional<SearchResponseCacheRecord> result = cache.remove(responseToken);
            if (result.isPresent()) {
                SearchResponseCacheRecord record = result.get();
                diagnostic.debug(() -> "Discarded cached search response " + responseToken
                        + " to " + record.username() + " for query '"
                        + record.query() + "' with token " + record.token());
                publishResponseFailed(record);
                return true;
            }
        } catch (Throwable failure) {
            diagnostic.warning(
                    () -> "Error removing cached search response " + responseToken + ": " + Failures.message(failure),
                    failure);
        }
        return false;
    }

    @Override
    public boolean tryRespond(String username, int token, String query) {
        publishRequestReceived(new SearchRequestEvent(username, token, query));

        SearchResponse response;
        try {
            // On this thread. It used to be dispatched and composed onto, so
            // that a blocking SPI could not stall the read loop that called
            // this; the dispatch is the caller's now, and it covers the
            // connect and the write as well as the catalog.
            response = Catalogs.searchResponse(
                    loggedInUsername.get(),
                    token,
                    catalog.get().search(dev.slsk.user.Username.of(username), query, MAXIMUM_MATCHES),
                    true,
                    advertisedUploadSpeed.getAsInt(),
                    0);
        } catch (Throwable failure) {
            warnResolution(username, token, query, failure);
            return false;
        }
        if (response.fileCount() + response.lockedFileCount() <= 0) {
            return false;
        }
        return deliverResolvedResponse(username, token, query, response);
    }

    @Override
    public boolean tryRespond(int responseToken) {
        SearchResponseCache cache = options.get().searchResponseCache();
        if (cache == null) {
            return false;
        }

        Optional<SearchResponseCacheRecord> lookup;
        try {
            lookup = cache.remove(responseToken);
        } catch (Throwable failure) {
            diagnostic.warning(
                    () -> "Error retrieving cached search response " + responseToken + ": " + Failures.message(failure),
                    failure);
            return false;
        }
        if (lookup.isEmpty()) {
            return false;
        }

        SearchResponseCacheRecord record = lookup.get();
        try {
            MessageConnection connection = peers.get().getCachedMessageConnection(record.username());
            connection.write(record.searchResponse().toByteArray());
            diagnostic.debug(() -> "Sent cached response " + responseToken
                    + " containing "
                    + totalFiles(record.searchResponse())
                    + " files to " + record.username()
                    + " for query '" + record.query()
                    + "' with token " + record.token());
            publishResponseDelivered(record);
            return true;
        } catch (Throwable failure) {
            Throwable cause = failure;
            diagnostic.debug(
                    () -> "Failed to send cached search response " + responseToken
                            + " to " + record.username() + " for query '"
                            + record.query() + "' with token " + record.token()
                            + ": " + Failures.message(cause),
                    cause);
            publishResponseFailed(record);
            return false;
        }
    }

    DiagnosticSink getDiagnostic() {
        return diagnostic;
    }

    private boolean deliverResolvedResponse(String username, int token, String query, SearchResponse response) {
        diagnostic.debug(() -> "Resolved " + response.fileCount() + " files for query '" + query + "' with token "
                + token + " from " + username);

        try {
            InetSocketAddress endpoint = endpoints.resolve(username, CancellationSignal.none());
            int responseToken = tokens.nextToken();
            MessageConnection connection;
            try {
                connection = peers.get()
                        .getOrAddMessageConnection(username, endpoint, responseToken, CancellationSignal.none());
            } catch (Throwable failure) {
                // Only a connection failure caches the response for later. A
                // write that fails on an established connection is a delivery
                // we already attempted, not one still owed.
                cacheUndelivered(responseToken, username, token, query, response);
                throw failure;
            }
            writeResponse(connection, response);
            diagnostic.debug(() -> "Sent response containing " + totalFiles(response)
                    + " files to " + username + " for query '" + query
                    + "' with token " + token);
            publishResponseDelivered(new SearchResponseCacheRecord(username, token, query, response));
            return true;
        } catch (Throwable failure) {
            Throwable cause = failure;
            diagnostic.debug(
                    () -> "Failed to send search response to " + username
                            + " for query '" + query + "' with token " + token
                            + ": " + Failures.message(cause),
                    cause);
            return false;
        }
    }

    private void writeResponse(MessageConnection connection, SearchResponse response) throws Exception {
        connection.write(response.toByteArray());
    }

    /**
     * Registers the eviction listener on the cache currently in force.
     *
     * <p>Bound here rather than in the constructor because the cache arrives
     * through an options supplier and options can be patched at runtime; a
     * response can only be evicted from a cache it was first put into, so
     * binding on the way in covers every instance that will ever hold one.
     */
    private void bindEvictionListener(SearchResponseCache cache) {
        if (evictionBoundTo == cache) {
            return;
        }
        synchronized (evictionBinding) {
            if (evictionBoundTo == cache) {
                return;
            }
            cache.setEvictionListener(this::onEvicted);
            evictionBoundTo = cache;
        }
    }

    /**
     * Reports a response that left the cache without reaching the peer who
     * searched.
     *
     * <p>Nothing tells us a solicitation failed, so most undelivered responses
     * end here rather than at a {@code CannotConnect}. Left silent, the failure
     * event reported a small fraction of the responses that were actually lost.
     */
    private void onEvicted(SearchResponseCacheRecord record) {
        diagnostic.debug(() -> "Expired undelivered search response to " + record.username() + " for query '"
                + record.query() + "' with token " + record.token());
        publishResponseFailed(record);
    }

    private void cacheUndelivered(
            int responseToken, String username, int token, String query, SearchResponse response) {
        SearchResponseCache cache = options.get().searchResponseCache();
        if (cache == null) {
            return;
        }
        bindEvictionListener(cache);
        try {
            cache.put(responseToken, new SearchResponseCacheRecord(username, token, query, response));
            diagnostic.debug(() -> "Failed to connect to " + username
                    + " with solicitation token " + responseToken
                    + " to deliver search results for query '" + query
                    + "' with token " + token
                    + ".  Cached response for potential delayed delivery.");
        } catch (Throwable failure) {
            diagnostic.warning(
                    () -> "Error caching undelivered search response "
                            + responseToken + " for query '" + query
                            + "' requested by " + username + " with token "
                            + token + ": " + Failures.message(failure),
                    failure);
        }
    }

    private void warnResolution(String username, int token, String query, Throwable failure) {
        diagnostic.warning(
                () -> "Error resolving search response for query '" + query
                        + "' requested by " + username + " with token " + token
                        + ": " + Failures.message(failure),
                failure);
    }

    private void publishDiagnostic(DiagnosticEvent eventData) {
        diagnosticListeners.forEach(listener -> listener.accept(eventData));
    }

    private void publishRequestReceived(SearchRequestEvent eventData) {
        requestListeners.forEach(listener -> listener.accept(eventData));
    }

    private void publishResponseDelivered(SearchResponseCacheRecord record) {
        SearchRequestResponseEvent eventData = toEvent(record);
        responseDeliveredListeners.forEach(listener -> listener.accept(eventData));
    }

    private void publishResponseFailed(SearchResponseCacheRecord record) {
        SearchRequestResponseEvent eventData = toEvent(record);
        responseFailedListeners.forEach(listener -> listener.accept(eventData));
    }

    private static SearchRequestResponseEvent toEvent(SearchResponseCacheRecord record) {
        return new SearchRequestResponseEvent(
                record.username(), record.token(), record.query(), record.searchResponse());
    }

    private static int totalFiles(SearchResponse response) {
        return response.fileCount() + response.lockedFileCount();
    }
}
