// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.Attachment;
import dev.slsk.EventStream;
import dev.slsk.Search;
import dev.slsk.events.SearchEvent;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.NetworkExecutor;
import dev.slsk.internal.common.Usernames;
import dev.slsk.internal.concurrent.BlockingInvocation;
import dev.slsk.internal.concurrent.CancellationController;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.concurrent.CancellationSubscription;
import dev.slsk.internal.events.EventBus;
import dev.slsk.internal.events.SearchRequestEvent;
import dev.slsk.internal.events.SearchRequestResponseEvent;
import dev.slsk.search.FileAttributeType;
import dev.slsk.search.FileAttributes;
import dev.slsk.search.SearchFile;
import dev.slsk.search.SearchFilters;
import dev.slsk.search.SearchId;
import dev.slsk.search.SearchQuery;
import dev.slsk.search.SearchResponse;
import dev.slsk.search.SearchResult;
import dev.slsk.search.SearchScope;
import dev.slsk.search.SearchSnapshot;
import dev.slsk.search.SearchStatus;
import dev.slsk.user.Username;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
 *
 * <p>Every search gets its own {@link CancellationController}, and the state
 * keeps it. That is what makes {@link #stop} mean something: without it a search
 * begun by {@link #start} could be marked cancelled but not actually stopped, and
 * a caller's own signal could not be told apart from the facet's.
 */
final class DefaultSearch implements Search {

    /**
     * How many finished searches are kept.
     *
     * <p>A snapshot holds every response it received, so an unbounded registry
     * is a session-length leak: a client left running for a day retains the full
     * result list of every search it ever ran. Finished searches are dropped
     * oldest-first past this many; running ones are never dropped.
     */
    static final int RETAINED_FINISHED_SEARCHES = 100;

    private final SoulseekEngine client;
    private final SearchDomain domain;
    private final EventBus<SearchEvent> events;
    private final Map<SearchId, State> searches = new ConcurrentHashMap<>();

    /**
     * Finished searches, oldest first, for the retention bound.
     *
     * <p>Touched only from inside the bus gate, which is where a search reaches
     * a terminal status, so it needs no lock of its own.
     */
    private final Deque<SearchId> finished = new ArrayDeque<>();

    private final AtomicLong revisions = new AtomicLong();

    DefaultSearch(SoulseekEngine client, EventBus<SearchEvent> events) {
        this.client = Objects.requireNonNull(client, "client");
        this.domain = client.searches();
        this.events = Objects.requireNonNull(events, "events");
        wire();
    }

    /** One running or finished search. */
    private static final class State {
        private final SearchId id;
        private final SearchQuery query;
        private final int token;
        private final Instant startedAt = Instant.now();
        private final List<SearchResponse> responses = new ArrayList<>();

        /**
         * Cancels the search-domain operation this search is running as.
         *
         * <p>Held rather than derived so that {@code stop} reaches the running
         * operation. A caller's own signal is chained onto this one rather than
         * passed down, so there is exactly one thing to cancel however the
         * search was begun.
         */
        private final CancellationController controller = new CancellationController();

        private volatile SearchStatus status = SearchStatus.IN_PROGRESS;
        private volatile Instant endedAt;
        private volatile long revision;

        State(SearchId id, SearchQuery query, int token) {
            this.id = id;
            this.query = query;
            this.token = token;
        }

        SearchSnapshot snapshot() {
            synchronized (responses) {
                return new SearchSnapshot(
                        id, query, status, startedAt, Optional.ofNullable(endedAt), List.copyOf(responses), revision);
            }
        }

        SearchResult result() {
            SearchSnapshot snapshot = snapshot();
            return new SearchResult(
                    id,
                    query,
                    snapshot.status(),
                    snapshot.responses(),
                    Duration.between(snapshot.startedAt(), snapshot.endedAt().orElseGet(Instant::now)));
        }
    }

    private void wire() {
        client.events().on(Kind.SEARCH_REQUEST_RECEIVED, (SearchRequestEvent event) -> {
            Username requester = event == null ? null : Usernames.fromWire(event.getUsername());
            if (requester != null) {
                events.publish(
                        new SearchEvent.RequestReceived(requester, event.getQuery(), event.getToken(), Instant.now()));
            }
        });
        client.events().on(Kind.SEARCH_RESPONSE_DELIVERED, (SearchRequestResponseEvent event) -> {
            Username requester = event == null ? null : Usernames.fromWire(event.getUsername());
            if (requester != null && event.getSearchResponse() != null) {
                dev.slsk.internal.search.SearchResponse delivered = event.getSearchResponse();
                events.publish(new SearchEvent.ResponseDelivered(
                        requester, event.getToken(), delivered.getFileCount(), Instant.now()));
            }
        });
        client.events().on(Kind.SEARCH_RESPONSE_DELIVERY_FAILED, (SearchRequestResponseEvent event) -> {
            Username requester = event == null ? null : Usernames.fromWire(event.getUsername());
            if (requester != null) {
                // Stackless: the internal event carries no cause, so this
                // exception is a message in the shape the public event wants.
                // Peers that vanish before delivery are routine, and a trace
                // here would point at this wiring, not at the failure.
                events.publish(new SearchEvent.ResponseDeliveryFailed(
                        requester,
                        event.getToken(),
                        Failures.stacklessIllegalState("could not deliver a search response to " + event.getUsername()),
                        Instant.now()));
            }
        });
    }

    // --- translation -------------------------------------------------------

    private static FileAttributes attributes(dev.slsk.internal.share.File file) {
        if (file.getAttributes() == null) {
            return FileAttributes.none();
        }
        Map<FileAttributeType, Integer> raw = new HashMap<>();
        for (dev.slsk.internal.share.FileAttribute attribute : file.getAttributes()) {
            FileAttributeType type =
                    FileAttributeType.fromCode(attribute.getType().getValue());
            if (type != null) {
                raw.put(type, attribute.getValue());
            }
        }
        return new FileAttributes(raw);
    }

    private static SearchFile file(dev.slsk.internal.share.File source) {
        return new SearchFile(source.getFilename(), source.getSize(), attributes(source));
    }

    private static SearchResponse response(dev.slsk.internal.search.SearchResponse source, SearchFilters filters) {
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

    private static dev.slsk.internal.search.SearchScope scope(SearchScope scope) {
        return switch (scope.kind()) {
            case NETWORK -> dev.slsk.internal.search.SearchScope.getNetwork();
            case WISHLIST -> dev.slsk.internal.search.SearchScope.getWishlist();
            case ROOM ->
                dev.slsk.internal.search.SearchScope.room(scope.targets().get(0));
            case USER ->
                dev.slsk.internal.search.SearchScope.user(scope.targets().toArray(new String[0]));
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
        State state = begin(query);
        // Returns the id, not the result: the search runs on a virtual thread of
        // its own and its responses reach the caller as events. `execute` never
        // throws, so nothing here can reach the thread's uncaught handler.
        NetworkExecutor.executor().execute(() -> {
            try {
                execute(state);
            } catch (InterruptedException interrupted) {
                // start() owns this library worker. Its interruption is a
                // lifecycle stop, not a public invocation to report.
                finish(state, SearchStatus.CANCELLED);
            }
        });
        return state.id;
    }

    @Override
    public SearchResult await(SearchId id) throws InterruptedException {
        return BlockingInvocation.run(signal -> await(id, signal));
    }

    @Override
    public SearchResult await(SearchId id, Duration timeout) throws InterruptedException, TimeoutException {
        return BlockingInvocation.run(client.getScheduler(), timeout, signal -> await(id, signal));
    }

    private SearchResult await(SearchId id, CancellationSignal signal) throws InterruptedException {
        Objects.requireNonNull(id, "id");
        State state = state(id);

        if (!state.status.isTerminal()) {
            CountDownLatch done = new CountDownLatch(1);
            try (dev.slsk.Subscription subscription = events.subscribe(SearchEvent.StatusChanged.class, event -> {
                        if (event.id().equals(id) && event.to().isTerminal()) {
                            done.countDown();
                        }
                    });
                    // This invocation owns only its wait. Explicit stop(id)
                    // remains the sole way to end an independently started
                    // search.
                    CancellationSubscription cancelled = signal.register(done::countDown)) {
                // Re-read after subscribing: it may have finished between the
                // two, and a wait that misses its own event never returns.
                if (!state.status.isTerminal()) {
                    waitFor(done, () -> state.status.isTerminal());
                }
            }
        }
        signal.throwIfCancellationRequested();
        return state.result();
    }

    @Override
    public SearchResult run(SearchQuery query) throws InterruptedException {
        return BlockingInvocation.run(signal -> run(query, signal));
    }

    @Override
    public SearchResult run(SearchQuery query, Duration timeout) throws InterruptedException, TimeoutException {
        return BlockingInvocation.run(client.getScheduler(), timeout, signal -> run(query, signal));
    }

    private SearchResult run(SearchQuery query, CancellationSignal signal) throws InterruptedException {
        Objects.requireNonNull(query, "query");
        State state = begin(query);
        // Chained rather than passed down, so `stop(id)` and the caller's own
        // signal cancel the same thing.
        try (CancellationSubscription linked = signal.register(state.controller::cancel)) {
            return execute(state);
        }
    }

    /** Registers a search and announces it, before anything is sent. */
    private State begin(SearchQuery query) {
        int token = client.getNextToken();
        SearchId id = SearchId.ofToken(token);
        State state = new State(id, query, token);
        searches.put(id, state);
        events.publish(
                new SearchEvent.StatusChanged(id, SearchStatus.IN_PROGRESS, SearchStatus.IN_PROGRESS, Instant.now()));
        return state;
    }

    /**
     * Runs a registered search to its terminal status.
     *
     * <p>Total by construction: it is the body of a virtual thread in {@link
     * #start}, and a search that fails is a search that ended, not an error to
     * propagate.
     */
    private SearchResult execute(State state) throws InterruptedException {
        SearchQuery query = state.query;
        CancellationSignal signal = state.controller.getSignal();
        SearchStatus status;
        try {
            domain.search(
                    dev.slsk.internal.search.SearchQuery.fromText(query.terms()),
                    source -> accept(state, source, query.filters()),
                    scope(query.scope()),
                    state.token,
                    options(query),
                    signal);
            status = SearchStatus.COMPLETED;
        } catch (RuntimeException exception) {
            InterruptedException interrupted = BlockingInvocation.interruption(exception);
            if (interrupted != null) {
                state.controller.cancel();
                finish(state, SearchStatus.CANCELLED);
                throw interrupted;
            }
            status = signal.isCancellationRequested() ? SearchStatus.CANCELLED : SearchStatus.TIMED_OUT;
        }
        finish(state, status);
        return state.result();
    }

    /**
     * Moves a search to its terminal status, once.
     *
     * <p>{@code stop} and the running operation both arrive here — stopping a
     * search makes its operation fail — so the transition is guarded and the
     * loser publishes nothing. Retirement happens in the same step, under the
     * bus gate, so an {@code attach} never sees a snapshot that disagrees with
     * the event that follows it.
     */
    private void finish(State state, SearchStatus status) {
        events.mutateAndPublish(() -> {
            if (state.status.isTerminal()) {
                return null;
            }
            SearchStatus previous = state.status;
            state.status = status;
            state.endedAt = Instant.now();
            retire(state.id);
            return new SearchEvent.StatusChanged(state.id, previous, status, Instant.now());
        });
    }

    /** Drops the oldest finished searches past the retention bound. */
    private void retire(SearchId id) {
        finished.addLast(id);
        while (finished.size() > RETAINED_FINISHED_SEARCHES) {
            searches.remove(finished.removeFirst());
        }
    }

    private State state(SearchId id) {
        State state = searches.get(id);
        if (state == null) {
            throw new IllegalArgumentException("unknown search: " + id);
        }
        return state;
    }

    private static void waitFor(CountDownLatch latch, java.util.function.BooleanSupplier completed)
            throws InterruptedException {
        try {
            latch.await(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            if (completed.getAsBoolean()) {
                Thread.currentThread().interrupt();
                return;
            }
            throw interrupted;
        }
    }

    /** Records a response and publishes it, keeping the snapshot and stream in step. */
    private void accept(State state, dev.slsk.internal.search.SearchResponse source, SearchFilters filters) {
        // The username is peer-supplied; a value no username can represent
        // drops the response rather than throwing out of the read dispatch.
        if (source == null || Usernames.fromWire(source.getUsername()) == null) {
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
        // Cancel the operation before flipping the status. The other order
        // published CANCELLED while the search carried on running, which is
        // the state a consumer trusts least.
        state.controller.cancel();
        finish(state, SearchStatus.CANCELLED);
    }

    @Override
    public SearchSnapshot get(SearchId id) {
        Objects.requireNonNull(id, "id");
        return state(id).snapshot();
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

    /**
     * Stops everything still running. Called when the client closes.
     *
     * <p>{@link #start} is the first thing here that owns a thread, so it is
     * also the first thing that could outlive the client that owns it.
     */
    void close() {
        for (State state : searches.values()) {
            if (!state.status.isTerminal()) {
                stop(state.id);
            }
        }
    }
}
