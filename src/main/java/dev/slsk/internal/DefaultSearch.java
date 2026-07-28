// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.Attachment;
import dev.slsk.CancellationSignal;
import dev.slsk.EventStream;
import dev.slsk.FileAttributeType;
import dev.slsk.FileAttributes;
import dev.slsk.Search;
import dev.slsk.SearchFile;
import dev.slsk.SearchFilters;
import dev.slsk.SearchId;
import dev.slsk.SearchQuery;
import dev.slsk.SearchResponse;
import dev.slsk.SearchResult;
import dev.slsk.SearchScope;
import dev.slsk.SearchSnapshot;
import dev.slsk.SearchStatus;
import dev.slsk.Username;
import dev.slsk.events.SearchEvent;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.common.Blocking;
import dev.slsk.internal.events.SearchRequestEvent;
import dev.slsk.internal.events.SearchRequestResponseEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * {@link Search}, over the engine.
 *
 * <p>Holds a snapshot per search so {@code get} and {@code active} can be
 * answered synchronously, and carries a revision counter so a consumer polling
 * can ask "did anything change?" with an integer comparison rather than by
 * diffing a two-hundred-entry response list.
 *
 * <p>Filtering happens as responses arrive rather than in the consumer, so a
 * rejected file never counts toward the response limit. Filtering afterwards
 * would work but would spend the limit on files the consumer was always going to
 * discard.
 */
final class DefaultSearch implements Search {

    private final SoulseekEngine client;
    private final SearchCoordinator coordinator;
    private final EventBus<SearchEvent> events;
    private final Map<SearchId, State> searches = new ConcurrentHashMap<>();
    private final AtomicLong revisions = new AtomicLong();

    DefaultSearch(SoulseekEngine client, EventBus<SearchEvent> events) {
        this.client = Objects.requireNonNull(client, "client");
        this.coordinator = client.searchCoordinator();
        this.events = Objects.requireNonNull(events, "events");
        wire();
    }

    /** One running or finished search. */
    private static final class State {
        private final SearchId id;
        private final SearchQuery query;
        private final Instant startedAt = Instant.now();
        private final List<SearchResponse> responses = new ArrayList<>();
        private volatile SearchStatus status = SearchStatus.IN_PROGRESS;
        private volatile Instant endedAt;
        private volatile long revision;

        State(SearchId id, SearchQuery query) {
            this.id = id;
            this.query = query;
        }

        SearchSnapshot snapshot() {
            synchronized (responses) {
                return new SearchSnapshot(
                        id, query, status, startedAt, Optional.ofNullable(endedAt), List.copyOf(responses), revision);
            }
        }
    }

    private void wire() {
        client.events().on(Kind.SEARCH_REQUEST_RECEIVED, (SearchRequestEvent event) -> {
            if (event != null) {
                events.publish(new SearchEvent.RequestReceived(
                        Username.of(event.getUsername()), event.getQuery(), event.getToken(), Instant.now()));
            }
        });
        client.events().on(Kind.SEARCH_RESPONSE_DELIVERED, (SearchRequestResponseEvent event) -> {
            if (event != null && event.getSearchResponse() != null) {
                dev.slsk.internal.SearchResponse delivered = event.getSearchResponse();
                events.publish(new SearchEvent.ResponseDelivered(
                        Username.of(event.getUsername()), event.getToken(), delivered.getFileCount(), Instant.now()));
            }
        });
        client.events().on(Kind.SEARCH_RESPONSE_DELIVERY_FAILED, (SearchRequestResponseEvent event) -> {
            if (event != null) {
                events.publish(new SearchEvent.ResponseDeliveryFailed(
                        Username.of(event.getUsername()),
                        event.getToken(),
                        new IllegalStateException("could not deliver a search response to " + event.getUsername()),
                        Instant.now()));
            }
        });
    }

    // --- translation -------------------------------------------------------

    private static FileAttributes attributes(dev.slsk.internal.File file) {
        if (file.getAttributes() == null) {
            return FileAttributes.none();
        }
        Map<FileAttributeType, Integer> raw = new HashMap<>();
        for (dev.slsk.internal.FileAttribute attribute : file.getAttributes()) {
            FileAttributeType type =
                    FileAttributeType.fromCode(attribute.getType().getValue());
            if (type != null) {
                raw.put(type, attribute.getValue());
            }
        }
        return new FileAttributes(raw);
    }

    private static SearchFile file(dev.slsk.internal.File source) {
        return new SearchFile(source.getFilename(), source.getSize(), attributes(source));
    }

    private static SearchResponse response(dev.slsk.internal.SearchResponse source, SearchFilters filters) {
        List<SearchFile> files = source.getFiles() == null
                ? List.of()
                : source.getFiles().stream()
                        .map(DefaultSearch::file)
                        .filter(candidate -> filters.accepts(candidate, false))
                        .toList();
        List<SearchFile> locked = source.getLockedFiles() == null
                ? List.of()
                : source.getLockedFiles().stream()
                        .map(DefaultSearch::file)
                        .filter(candidate -> filters.accepts(candidate, true))
                        .toList();
        return new SearchResponse(
                Username.of(source.getUsername()),
                source.hasFreeUploadSlot() ? 1 : 0,
                source.getUploadSpeed(),
                source.getQueueLength(),
                files,
                locked);
    }

    private static dev.slsk.internal.SearchScope scope(SearchScope scope) {
        return switch (scope.kind()) {
            case NETWORK -> dev.slsk.internal.SearchScope.getNetwork();
            case WISHLIST -> dev.slsk.internal.SearchScope.getWishlist();
            case ROOM -> dev.slsk.internal.SearchScope.room(scope.targets().get(0));
            case USER -> dev.slsk.internal.SearchScope.user(scope.targets().toArray(new String[0]));
        };
    }

    private static dev.slsk.internal.options.SearchOptions options(SearchQuery query) {
        return new dev.slsk.internal.options.SearchOptions(
                (int) query.limits().overall().toMillis(), query.limits().maxResponses(), false, 1);
    }

    // --- operations --------------------------------------------------------

    @Override
    public SearchId start(SearchQuery query) {
        Objects.requireNonNull(query, "query");
        return run(query, CancellationSignal.none()).id();
    }

    @Override
    public SearchResult await(SearchId id, CancellationSignal signal) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(signal, "signal");
        State state = searches.get(id);
        if (state == null) {
            throw new IllegalArgumentException("unknown search: " + id);
        }
        SearchSnapshot snapshot = state.snapshot();
        return new SearchResult(
                id,
                snapshot.query(),
                snapshot.status(),
                snapshot.responses(),
                Duration.between(snapshot.startedAt(), snapshot.endedAt().orElseGet(Instant::now)));
    }

    @Override
    public SearchResult run(SearchQuery query, CancellationSignal signal) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(signal, "signal");

        int token = client.getNextToken();
        SearchId id = SearchId.ofToken(token);
        State state = new State(id, query);
        searches.put(id, state);

        events.publish(
                new SearchEvent.StatusChanged(id, SearchStatus.IN_PROGRESS, SearchStatus.IN_PROGRESS, Instant.now()));

        Instant began = Instant.now();
        SearchStatus status;
        try {
            Blocking.await(coordinator.search(
                    dev.slsk.internal.SearchQuery.fromText(query.terms()),
                    source -> accept(state, source, query.filters()),
                    scope(query.scope()),
                    token,
                    options(query),
                    signal));
            status = SearchStatus.COMPLETED;
        } catch (RuntimeException exception) {
            status = signal.isCancellationRequested() ? SearchStatus.CANCELLED : SearchStatus.TIMED_OUT;
        }

        SearchStatus previous = state.status;
        state.status = status;
        state.endedAt = Instant.now();
        events.publish(new SearchEvent.StatusChanged(id, previous, status, Instant.now()));

        SearchSnapshot snapshot = state.snapshot();
        return new SearchResult(id, query, status, snapshot.responses(), Duration.between(began, state.endedAt));
    }

    /** Records a response and publishes it, keeping the snapshot and stream in step. */
    private void accept(State state, dev.slsk.internal.SearchResponse source, SearchFilters filters) {
        if (source == null || source.getUsername() == null) {
            return;
        }
        SearchResponse response = response(source, filters);
        if (response.fileCount() == 0) {
            return;
        }
        events.mutateAndPublish(() -> {
            long revision = revisions.incrementAndGet();
            synchronized (state.responses) {
                state.responses.add(response);
                state.revision = revision;
            }
            return new SearchEvent.ResponsesReceived(state.id, List.of(response), revision, Instant.now());
        });
    }

    @Override
    public void stop(SearchId id) {
        Objects.requireNonNull(id, "id");
        State state = searches.get(id);
        if (state == null || state.status.isTerminal()) {
            return;
        }
        events.mutateAndPublish(() -> {
            SearchStatus previous = state.status;
            state.status = SearchStatus.CANCELLED;
            state.endedAt = Instant.now();
            return new SearchEvent.StatusChanged(id, previous, SearchStatus.CANCELLED, Instant.now());
        });
    }

    @Override
    public SearchSnapshot get(SearchId id) {
        Objects.requireNonNull(id, "id");
        State state = searches.get(id);
        if (state == null) {
            throw new IllegalArgumentException("unknown search: " + id);
        }
        return state.snapshot();
    }

    @Override
    public List<SearchSnapshot> active() {
        return searches.values().stream()
                .filter(state -> !state.status.isTerminal())
                .map(State::snapshot)
                .toList();
    }

    @Override
    public EventStream<SearchEvent> events() {
        return events;
    }

    @Override
    public Attachment<List<SearchSnapshot>> attach(Consumer<SearchEvent> listener) {
        return events.attach(this::active, listener);
    }
}
