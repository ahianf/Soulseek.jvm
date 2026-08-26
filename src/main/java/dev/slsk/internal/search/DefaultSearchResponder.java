// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import dev.slsk.Subscription;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.TokenFactory;
import dev.slsk.internal.concurrent.CancellationSignal;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Responds to incoming search requests. */
public final class DefaultSearchResponder implements SearchResponder {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSearchResponder.class);

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

    private final CopyOnWriteArrayList<Consumer<? super SearchRequestEvent>> requestListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super SearchRequestResponseEvent>> responseDeliveredListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super SearchRequestResponseEvent>> responseFailedListeners =
            new CopyOnWriteArrayList<>();
    private final Object evictionBinding = new Object();
    private volatile SearchResponseCache evictionBoundTo;

    /** Creates a responder. */
    public DefaultSearchResponder(
            Supplier<SoulseekClientOptions> options,
            Supplier<PeerConnectionManager> peers,
            TokenFactory tokens,
            Endpoints endpoints,
            Supplier<dev.slsk.spi.ShareCatalog> catalog,
            Supplier<String> loggedInUsername,
            java.util.function.IntSupplier advertisedUploadSpeed) {
        this.options = Objects.requireNonNull(options, "options");
        this.peers = Objects.requireNonNull(peers, "peers");
        this.tokens = Objects.requireNonNull(tokens, "tokens");
        this.endpoints = Objects.requireNonNull(endpoints, "endpoints");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.loggedInUsername = Objects.requireNonNull(loggedInUsername, "loggedInUsername");
        this.advertisedUploadSpeed = Objects.requireNonNull(advertisedUploadSpeed, "advertisedUploadSpeed");
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
                LOG.debug(
                        "Discarded cached search response {} to {} for query '{}' with token {}",
                        responseToken,
                        record.username(),
                        record.query(),
                        record.token());
                publishResponseFailed(record);
                return true;
            }
        } catch (Throwable failure) {
            LOG.warn(
                    "Error removing cached search response {}: {}",
                    responseToken,
                    Failures.message(failure),
                    failure);
        }
        return false;
    }

    @Override
    public boolean tryRespond(String username, int token, String query) {
        publishRequestReceived(new SearchRequestEvent(username, token, query));

        SearchResponseMessage response;
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
            LOG.warn(
                    "Error retrieving cached search response {}: {}",
                    responseToken,
                    Failures.message(failure),
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
            LOG.debug(
                    "Sent cached response {} containing {} files to {} for query '{}' with token {}",
                    responseToken,
                    totalFiles(record.searchResponse()),
                    record.username(),
                    record.query(),
                    record.token());
            publishResponseDelivered(record);
            return true;
        } catch (Throwable failure) {
            Throwable cause = failure;
            LOG.debug(
                    "Failed to send cached search response {} to {} for query '{}' with token {}: {}",
                    responseToken,
                    record.username(),
                    record.query(),
                    record.token(),
                    Failures.message(cause),
                    cause);
            publishResponseFailed(record);
            return false;
        }
    }

    private boolean deliverResolvedResponse(String username, int token, String query, SearchResponseMessage response) {
        LOG.debug(
                "Resolved {} files for query '{}' with token {} from {}",
                response.fileCount(),
                query,
                token,
                username);

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
            LOG.debug(
                    "Sent response containing {} files to {} for query '{}' with token {}",
                    totalFiles(response),
                    username,
                    query,
                    token);
            publishResponseDelivered(new SearchResponseCacheRecord(username, token, query, response));
            return true;
        } catch (Throwable failure) {
            Throwable cause = failure;
            LOG.debug(
                    "Failed to send search response to {} for query '{}' with token {}: {}",
                    username,
                    query,
                    token,
                    Failures.message(cause),
                    cause);
            return false;
        }
    }

    private void writeResponse(MessageConnection connection, SearchResponseMessage response) throws Exception {
        connection.write(response.toByteArray());
    }

    /**
     * Registers the eviction listener on the cache currently in force.
     *
     * <p>Bound here rather than in the constructor because the cache arrives
     * through an options supplier. A response can only be evicted from a cache
     * it was first put into, so binding on the way in covers the configured
     * instance without adding constructor coupling.
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
        LOG.debug(
                "Expired undelivered search response to {} for query '{}' with token {}",
                record.username(),
                record.query(),
                record.token());
        publishResponseFailed(record);
    }

    private void cacheUndelivered(
            int responseToken, String username, int token, String query, SearchResponseMessage response) {
        SearchResponseCache cache = options.get().searchResponseCache();
        if (cache == null) {
            return;
        }
        bindEvictionListener(cache);
        try {
            cache.put(responseToken, new SearchResponseCacheRecord(username, token, query, response));
            LOG.debug(
                    "Failed to connect to {} with solicitation token {} to deliver search results for query '{}' "
                            + "with token {}. Cached response for potential delayed delivery.",
                    username,
                    responseToken,
                    query,
                    token);
        } catch (Throwable failure) {
            LOG.warn(
                    "Error caching undelivered search response {} for query '{}' requested by {} with token {}: {}",
                    responseToken,
                    query,
                    username,
                    token,
                    Failures.message(failure),
                    failure);
        }
    }

    private void warnResolution(String username, int token, String query, Throwable failure) {
        LOG.warn(
                "Error resolving search response for query '{}' requested by {} with token {}: {}",
                query,
                username,
                token,
                Failures.message(failure),
                failure);
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

    private static int totalFiles(SearchResponseMessage response) {
        return response.fileCount() + response.lockedFileCount();
    }
}
