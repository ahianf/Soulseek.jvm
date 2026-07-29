// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import dev.slsk.CancellationSignal;
import dev.slsk.CancellationSubscription;
import dev.slsk.internal.Search;
import dev.slsk.internal.SearchQuery;
import dev.slsk.internal.SearchResponse;
import dev.slsk.internal.SearchScope;
import dev.slsk.internal.SearchState;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.NetworkExecutor;
import dev.slsk.internal.common.Scheduler;
import dev.slsk.internal.common.Settlement;
import dev.slsk.internal.options.SearchOptions;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
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
    /**
     * The waits in progress, and the terminal outcome they will all be given.
     *
     * <p>One {@link Settlement} per waiter rather than one shared cell, because
     * a caller can abandon its own wait — the {@code CancellationSignal} passed
     * to {@link #waitForCompletion} cancels the waiting, not the search — and
     * the rest of the waiters must stay in it. {@code terminal} is what a waiter
     * arriving after the search has already ended settles on immediately, and
     * what makes registering and reading the state under the same lock enough
     * to close the gap between them.
     */
    private final Set<Settlement> waiters = ConcurrentHashMap.newKeySet();

    private volatile Boolean terminal;
    private final AtomicBoolean disposed = new AtomicBoolean();
    private final AtomicInteger fileCount = new AtomicInteger();
    private final AtomicInteger lockedFileCount = new AtomicInteger();
    private final SearchOptions options;
    private final SearchQuery query;
    private final AtomicInteger responseCount = new AtomicInteger();
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private final SearchScope scope;
    private final Scheduler timerExecutor;
    private final boolean ownsScheduler;
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
        this(query, scope, token, options, null);
    }

    /**
     * Creates a search sharing a caller-owned scheduler.
     *
     * @param query the search query
     * @param scope the search scope
     * @param token the search token
     * @param options the search options
     * @param scheduler the shared scheduler, or {@code null} to own one
     */
    public SearchInternal(SearchQuery query, SearchScope scope, int token, SearchOptions options, Scheduler scheduler) {
        this.query = query;
        this.scope = scope;
        this.token = token;
        this.options = options == null ? new SearchOptions() : options;
        if (this.options.getSearchTimeout() <= 0) {
            throw new IllegalArgumentException("searchTimeout must be greater than zero");
        }
        this.ownsScheduler = scheduler == null;
        this.timerExecutor = scheduler == null ? new Scheduler("soulseek-search-timeout-" + token) : scheduler;
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
            settleWaiters(false);
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
            settleWaiters(true);
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

                    List<dev.slsk.internal.File> files = stream(response.getFiles())
                            .filter(file -> options.getFileFilter() == null
                                    || options.getFileFilter().test(file))
                            .toList();
                    List<dev.slsk.internal.File> lockedFiles = stream(response.getLockedFiles())
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

    /**
     * Waits until this search completes or the caller gives up.
     *
     * <p>Cancelling abandons this caller's wait, not the search: another caller
     * may still be in it, and the search itself carries on.
     *
     * <p>This was the one future left in the search state, because handing one
     * terminal outcome to every waiter at once is what a future does and a
     * single blocking cell does not. A cell per waiter does, and it is the
     * abandonment that makes it the honest shape: what each caller owns is its
     * own wait.
     *
     * @param cancellationSignal abandons this wait when signalled
     */
    public void waitForCompletion(CancellationSignal cancellationSignal) {
        Objects.requireNonNull(cancellationSignal, "cancellationSignal");
        Settlement wait = new Settlement();
        // Registered and the terminal state read under the same lock the
        // terminal transition takes, so a search that ends between the two is
        // not waited on forever.
        stateLock.writeLock().lock();
        try {
            waiters.add(wait);
            settle(wait, terminal);
        } finally {
            stateLock.writeLock().unlock();
        }

        CancellationSubscription registration =
                cancellationSignal.register(() -> wait.fail(new CancellationException("Operation cancelled")));
        try {
            Throwable failure = wait.await();
            if (failure != null) {
                throw Failures.propagate(failure);
            }
        } finally {
            registration.close();
            waiters.remove(wait);
        }
    }

    /** Waits without a cancellable caller token. */
    public void waitForCompletion() {
        waitForCompletion(CancellationSignal.none());
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
            if (ownsScheduler) {
                timerExecutor.close();
            }
        }
    }

    /** Hands the terminal outcome to every waiter, and to every later one. */
    private void settleWaiters(boolean completed) {
        if (terminal != null) {
            return;
        }
        terminal = completed;
        waiters.forEach(wait -> settle(wait, completed));
    }

    private static void settle(Settlement wait, Boolean completed) {
        if (completed == null) {
            return;
        }
        if (completed) {
            wait.succeed();
        } else {
            wait.fail(new CancellationException("Operation cancelled"));
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
        // Time out on the scheduler, but run completion (which raises the state-changed
        // event and the caller's stateChanged callback) on a virtual thread so a blocking
        // callback cannot stall this timer thread.
        timeoutTask = timerExecutor.schedule(
                () -> NetworkExecutor.executor().execute(() -> complete(SearchState.TIMED_OUT)),
                options.getSearchTimeout(),
                TimeUnit.MILLISECONDS);
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
