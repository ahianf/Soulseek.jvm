// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.search;

import dev.slsk.CancellationSignal;
import dev.slsk.CancellationSubscription;
import dev.slsk.Search;
import dev.slsk.SearchQuery;
import dev.slsk.SearchResponse;
import dev.slsk.SearchScope;
import dev.slsk.SearchState;
import dev.slsk.options.SearchOptions;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** The mutable internal state of a single file search. */
public final class SearchInternal implements AutoCloseable {
    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    private final AtomicBoolean disposed = new AtomicBoolean();
    private final AtomicInteger fileCount = new AtomicInteger();
    private final AtomicInteger lockedFileCount = new AtomicInteger();
    private final SearchOptions options;
    private final SearchQuery query;
    private final AtomicInteger responseCount = new AtomicInteger();
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private final SearchScope scope;
    private final ScheduledExecutorService timerExecutor;
    private final int token;

    private volatile Consumer<SearchResponse> responseReceived;
    private volatile ScheduledFuture<?> timeoutTask;
    private volatile SearchState state = SearchState.NONE;

    /** Creates a search using default options. */
    public SearchInternal(SearchQuery query, SearchScope scope, int token) {
        this(query, scope, token, null);
    }

    /** Creates a search. */
    public SearchInternal(SearchQuery query, SearchScope scope, int token, SearchOptions options) {
        this.query = query;
        this.scope = scope;
        this.token = token;
        this.options = options == null ? new SearchOptions() : options;
        if (this.options.getSearchTimeout() <= 0) {
            throw new IllegalArgumentException("searchTimeout must be greater than zero");
        }
        timerExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "soulseek-search-timeout-" + token);
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Returns the total received file count. */
    public int getFileCount() {
        return fileCount.get();
    }

    /** Returns the total received locked-file count. */
    public int getLockedFileCount() {
        return lockedFileCount.get();
    }

    /** Returns this search's options. */
    public SearchOptions getOptions() {
        return options;
    }

    /** Returns the search query. */
    public SearchQuery getQuery() {
        return query;
    }

    /** Returns the accepted response count. */
    public int getResponseCount() {
        return responseCount.get();
    }

    /** Returns the search scope. */
    public SearchScope getScope() {
        return scope;
    }

    /** Returns the current state. */
    public SearchState getState() {
        return state;
    }

    /** Returns the search token. */
    public int getToken() {
        return token;
    }

    /** Returns whether the timeout task is currently active. */
    boolean isTimeoutActive() {
        ScheduledFuture<?> task = timeoutTask;
        return task != null && !task.isDone();
    }

    /** Replaces the response callback. */
    public void setResponseReceived(Consumer<SearchResponse> callback) {
        responseReceived = callback;
    }

    /** Adds a response callback using C# delegate-composition order. */
    public synchronized void addResponseReceived(Consumer<SearchResponse> callback) {
        Objects.requireNonNull(callback, "callback");
        Consumer<SearchResponse> existing = responseReceived;
        responseReceived = existing == null ? callback : existing.andThen(callback);
    }

    /** Cancels the search. */
    public void cancel() {
        stateLock.writeLock().lock();
        try {
            stopTimeout();
            state = SearchState.COMPLETED.or(SearchState.CANCELLED);
            completion.cancel(false);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /** Completes the search with a terminal detail state. */
    public void complete(SearchState terminalState) {
        Objects.requireNonNull(terminalState, "terminalState");
        stateLock.writeLock().lock();
        try {
            stopTimeout();
            state = SearchState.COMPLETED.or(terminalState);
            completion.complete(null);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /** Sets the current search state. */
    public void setState(SearchState newState) {
        Objects.requireNonNull(newState, "newState");
        stateLock.writeLock().lock();
        try {
            SearchState previousState = state;
            state = newState;
            if (!previousState.equals(SearchState.IN_PROGRESS) && newState.equals(SearchState.IN_PROGRESS)) {
                resetTimeout();
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * Applies response filters and accepts a response when eligible.
     *
     * @throws IllegalArgumentException when the response token differs
     */
    public void tryAddResponse(SearchResponse initialResponse) {
        Objects.requireNonNull(initialResponse, "response");
        if (initialResponse.getToken() != token) {
            throw new IllegalArgumentException("Search for '" + query + "' with token " + token
                    + " received response with search token "
                    + initialResponse.getToken());
        }
        if (disposed.get()) {
            return;
        }

        SearchResponse response = initialResponse;
        try {
            stateLock.readLock().lock();
            try {
                if (!state.contains(SearchState.IN_PROGRESS) || !responseMeetsOptionCriteria(response)) {
                    return;
                }

                if (options.isFilterResponses()) {
                    if (options.getResponseFilter() != null
                            && !options.getResponseFilter().test(response)) {
                        return;
                    }

                    List<dev.slsk.File> files = stream(response.getFiles())
                            .filter(file -> options.getFileFilter() == null
                                    || options.getFileFilter().test(file))
                            .toList();
                    List<dev.slsk.File> lockedFiles = stream(response.getLockedFiles())
                            .filter(file -> options.getFileFilter() == null
                                    || options.getFileFilter().test(file))
                            .toList();
                    response = new SearchResponse(
                            response.getUsername(),
                            response.getToken(),
                            response.hasFreeUploadSlot(),
                            response.getUploadSpeed(),
                            response.getQueueLength(),
                            files,
                            lockedFiles);

                    if (response.getFileCount() + response.getLockedFileCount()
                            < options.getMinimumResponseFileCount()) {
                        return;
                    }
                }

                responseCount.incrementAndGet();
                fileCount.addAndGet(response.getFileCount());
                lockedFileCount.addAndGet(response.getLockedFileCount());

                Consumer<SearchResponse> callback = responseReceived;
                if (callback != null) {
                    callback.accept(response);
                }
                resetTimeout();
            } finally {
                stateLock.readLock().unlock();
            }

            if (responseCount.get() >= options.getResponseLimit()) {
                complete(SearchState.RESPONSE_LIMIT_REACHED);
            } else if (fileCount.get() >= options.getFileLimit()) {
                complete(SearchState.FILE_LIMIT_REACHED);
            }
        } catch (IllegalStateException ignored) {
            // Java adaptation of the source's late ObjectDisposedException.
        }
    }

    /** Waits until this search completes or the caller cancels. */
    public CompletableFuture<Void> waitForCompletion(CancellationSignal cancellationSignal) {
        Objects.requireNonNull(cancellationSignal, "cancellationSignal");
        CompletableFuture<Void> result = new CompletableFuture<>();
        CancellationSubscription registration = cancellationSignal.register(
                () -> result.completeExceptionally(new CancellationException("Operation cancelled")));
        completion.whenComplete((ignored, failure) -> {
            if (failure == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(failure);
            }
        });
        result.whenComplete((ignored, failure) -> registration.close());
        return result;
    }

    /** Waits without a cancellable caller token. */
    public CompletableFuture<Void> waitForCompletion() {
        return waitForCompletion(CancellationSignal.none());
    }

    /** Creates the public immutable snapshot of this search. */
    public Search toSearch() {
        return new Search(query, scope, token, state, responseCount.get(), fileCount.get(), lockedFileCount.get());
    }

    /** Releases the timeout task and executor. */
    @Override
    public void close() {
        if (disposed.compareAndSet(false, true)) {
            stopTimeout();
            timerExecutor.shutdownNow();
        }
    }

    private boolean responseMeetsOptionCriteria(SearchResponse response) {
        return !options.isFilterResponses()
                || (response.getFileCount() + response.getLockedFileCount() >= options.getMinimumResponseFileCount()
                        && response.getUploadSpeed() >= options.getMinimumPeerUploadSpeed()
                        && response.getQueueLength() < options.getMaximumPeerQueueLength());
    }

    private void resetTimeout() {
        stopTimeout();
        timeoutTask = timerExecutor.schedule(
                () -> complete(SearchState.TIMED_OUT), options.getSearchTimeout(), TimeUnit.MILLISECONDS);
    }

    private void stopTimeout() {
        ScheduledFuture<?> task = timeoutTask;
        if (task != null) {
            task.cancel(false);
            timeoutTask = null;
        }
    }

    private static <T> Stream<T> stream(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
