// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.CancellationController;
import dev.slsk.Download;
import dev.slsk.DownloadPolicy;
import dev.slsk.DownloadRequest;
import dev.slsk.Priority;
import dev.slsk.TransferId;
import dev.slsk.TransferOutcome;
import dev.slsk.TransferState;
import dev.slsk.Username;
import dev.slsk.internal.common.NetworkExecutor;
import dev.slsk.internal.common.Scheduler;
import dev.slsk.spi.TransferStore;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * The queue the library owns, and the reason 1.0 exists.
 *
 * <p>Every application built one of these. {@code tenine} had four hundred and
 * twenty-eight lines of registry, position service and upload queue, none of it
 * specific to {@code tenine}, all of it written from the protocol's shape
 * outward. This is that machinery written once, in the place that knows the
 * protocol.
 *
 * <p>The design that makes it testable is the split between deciding and doing.
 * This class decides: which transfer runs next, when a peer has had enough
 * connections at once, whether a refusal is worth waiting out and how long. A
 * {@link Runner} does: it talks to a peer and comes back with an outcome. In
 * production the runner is the transfer engine; in a test it is a lambda, and
 * every scheduling rule below can be asserted without a socket.
 *
 * <p>Admission is under one lock and is the only place slots are counted. The
 * alternative — a semaphore per user plus a global one — is where per-user caps
 * go wrong, because acquiring two in sequence is not atomic and the pair can be
 * held by two transfers that each got one.
 */
final class DownloadQueue {

    /**
     * Asks a peer where we are in its queue.
     *
     * <p>Separate from {@link Runner} because it is asked <em>while</em> a run
     * is in flight, on the scheduler rather than on the transfer's own thread.
     */
    @FunctionalInterface
    interface PositionProbe {
        /**
         * Returns the peer's place-in-queue for one download.
         *
         * @param entry which download
         * @return the position, or empty if the peer did not say
         */
        java.util.OptionalInt place(Entry entry);
    }

    /** What actually moves the bytes. */
    @FunctionalInterface
    interface Runner {
        /**
         * Runs one attempt, blocking until it reaches a terminal state.
         *
         * @param entry what to fetch
         * @return how the attempt ended
         */
        TransferOutcome run(Entry entry);
    }

    /** One download, from enqueue to terminal state. */
    static final class Entry {

        private final TransferId id;
        private final DownloadRequest request;
        private final Instant enqueuedAt = Instant.now();
        private final AtomicReference<CancellationController> cancellation =
                new AtomicReference<>(new CancellationController());
        private volatile TransferState state = new TransferState.Queued(0);
        private volatile Priority priority;
        private volatile int attempt;
        private volatile Instant startedAt;
        private volatile Instant endedAt;

        /**
         * How many bytes the last attempt left on disk.
         *
         * <p>Recorded by the runner rather than asked of the sink, because what
         * matters is what was written, and only the thing that wrote it knows.
         */
        private volatile long resumeOffset;
        /** Set while paused, so resume knows there is nothing running to stop. */
        private final AtomicBoolean paused = new AtomicBoolean();

        Entry(TransferId id, DownloadRequest request) {
            this.id = id;
            this.request = request;
            this.priority = request.priority();
        }

        TransferId id() {
            return id;
        }

        DownloadRequest request() {
            return request;
        }

        Username user() {
            return request.user();
        }

        int attempt() {
            return attempt;
        }

        /**
         * Returns where the next attempt should ask the peer to start.
         *
         * @return the offset, or zero for a fresh download
         */
        long resumeOffset() {
            return resumeOffset;
        }

        /**
         * Records what an attempt left behind, for the next one to resume from.
         *
         * @param value bytes written to the sink
         */
        void resumeOffset(long value) {
            resumeOffset = Math.max(0, value);
        }

        dev.slsk.CancellationSignal signal() {
            return cancellation.get().getSignal();
        }

        Download snapshot() {
            return new Download(
                    id,
                    request.user(),
                    request.path(),
                    request.expectedSize(),
                    state,
                    priority,
                    enqueuedAt,
                    Optional.ofNullable(startedAt),
                    Optional.ofNullable(endedAt),
                    Math.max(1, attempt),
                    request.tags());
        }

        boolean isTerminal() {
            return state instanceof TransferState.Finished;
        }

        boolean isRunning() {
            return !(state instanceof TransferState.Queued)
                    && !(state instanceof TransferState.Paused)
                    && !isTerminal();
        }
    }

    private final Object lock = new Object();

    /**
     * Serialises writes to the store, separately from admission.
     *
     * <p>The snapshot is taken inside this monitor rather than passed into it,
     * so whichever thread enters last writes the newest state. Two threads
     * racing — an admission on one and a completion on another — otherwise write
     * in whatever order they are scheduled, and a store can end up believing a
     * finished download is still starting.
     *
     * <p>Separate from {@link #lock} because a store is a consumer's code and
     * may be a database. Holding the admission lock across it would let a slow
     * write stall the queue.
     */
    private final Object storeLock = new Object();

    private final Map<TransferId, Entry> entries = new LinkedHashMap<>();
    private final Scheduler scheduler;
    private final Runner runner;
    private final TransferStore store;

    /** Called with (entry, previous state) whenever a state changes. */
    private final BiConsumer<Entry, TransferState> onStateChanged;

    /** Called with (entry, when the next attempt is due) before a retry waits. */
    private volatile BiConsumer<Entry, Instant> onRetryScheduled = (entry, at) -> {};

    /** How the peer's place-in-queue is asked for; nothing polls until one is set. */
    private volatile PositionProbe probe;

    /** The running poll, cancelled and replaced whenever the interval changes. */
    private volatile java.util.concurrent.ScheduledFuture<?> poll;

    private volatile DownloadPolicy policy = DownloadPolicy.defaults();
    private final AtomicBoolean closed = new AtomicBoolean();

    DownloadQueue(
            Scheduler scheduler, Runner runner, TransferStore store, BiConsumer<Entry, TransferState> onStateChanged) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.runner = Objects.requireNonNull(runner, "runner");
        this.store = Objects.requireNonNull(store, "store");
        this.onStateChanged = Objects.requireNonNull(onStateChanged, "onStateChanged");
    }

    /** Called with (entry, the peer's new position) when a poll finds a change. */
    private volatile BiConsumer<Entry, java.util.OptionalInt> onPositionChanged = (entry, place) -> {};

    /**
     * Sets who hears about a queue-position change.
     *
     * @param listener called when a poll finds the peer has moved us
     */
    void onPositionChanged(BiConsumer<Entry, java.util.OptionalInt> listener) {
        onPositionChanged = Objects.requireNonNull(listener, "listener");
    }

    /**
     * Sets who hears about a scheduled retry.
     *
     * <p>Separate from the state-change callback because a retry is not a state:
     * the download goes back to {@code Queued} either way, and what a consumer
     * needs to know is that it will not be tried again for another four minutes.
     *
     * @param listener called before each retry's backoff begins
     */
    void onRetryScheduled(BiConsumer<Entry, Instant> listener) {
        onRetryScheduled = Objects.requireNonNull(listener, "listener");
    }

    DownloadPolicy policy() {
        return policy;
    }

    void policy(DownloadPolicy value) {
        DownloadPolicy previous = policy;
        policy = Objects.requireNonNull(value, "policy");
        if (!previous.queuePositionPollInterval().equals(value.queuePositionPollInterval())) {
            startPolling();
        }
        // A raised cap should take effect now rather than at the next
        // completion, or widening the queue does nothing until something ends.
        admit();
    }

    /**
     * Sets how the peer's place-in-queue is asked for, and starts asking.
     *
     * @param value the probe
     */
    void positionProbe(PositionProbe value) {
        probe = Objects.requireNonNull(value, "probe");
        startPolling();
    }

    /**
     * Records a state the engine reported for a running download.
     *
     * <p>The queue owns {@code Queued}, {@code Paused} and {@code Finished};
     * everything between them belongs to the transfer itself, and without this
     * a download would read as {@code Requesting} for its entire life. Applied
     * only while the queue believes the download is running, so a state arriving
     * late cannot resurrect something already cancelled.
     *
     * @param id which download
     * @param observed what the engine says it is doing
     */
    void observed(TransferId id, TransferState observed) {
        Entry entry = entry(id);
        if (entry == null || !entry.isRunning() || observed instanceof TransferState.Finished) {
            return;
        }
        transition(entry, observed);
    }

    /**
     * Restarts the queue-position poll.
     *
     * <p>One timer for the whole queue rather than one per download: a hundred
     * queued transfers against ten peers is ten questions every thirty seconds,
     * not a hundred timers.
     */
    private void startPolling() {
        java.util.concurrent.ScheduledFuture<?> existing = poll;
        if (existing != null) {
            existing.cancel(false);
        }
        if (probe == null || closed.get()) {
            return;
        }
        long millis = Math.max(1, policy.queuePositionPollInterval().toMillis());
        poll = scheduler.scheduleAtFixedRate(this::pollPositions, millis, millis, TimeUnit.MILLISECONDS);
    }

    /** Asks each remotely-queued peer where we are, and publishes any change. */
    private void pollPositions() {
        PositionProbe current = probe;
        if (current == null || closed.get()) {
            return;
        }
        List<Entry> waiting = new ArrayList<>();
        synchronized (lock) {
            for (Entry entry : entries.values()) {
                if (entry.state instanceof TransferState.QueuedRemotely) {
                    waiting.add(entry);
                }
            }
        }
        for (Entry entry : waiting) {
            java.util.OptionalInt place;
            try {
                place = current.place(entry);
            } catch (RuntimeException unreachable) {
                // A peer that will not answer is not a reason to stop asking the
                // rest, and the position we have is still the last one it gave.
                continue;
            }
            if (!(entry.state instanceof TransferState.QueuedRemotely known)) {
                continue;
            }
            if (known.position().equals(place)) {
                continue;
            }
            transition(entry, new TransferState.QueuedRemotely(place, Instant.now()));
            onPositionChanged.accept(entry, place);
        }
    }

    // --- intents -----------------------------------------------------------

    Entry enqueue(TransferId id, DownloadRequest request) {
        Entry entry = new Entry(id, request);
        synchronized (lock) {
            entries.put(id, entry);
        }
        save(entry);
        admit();
        return entry;
    }

    Optional<Entry> find(TransferId id) {
        synchronized (lock) {
            return Optional.ofNullable(entries.get(id));
        }
    }

    List<Download> all() {
        synchronized (lock) {
            List<Download> snapshot = new ArrayList<>(entries.size());
            for (Entry entry : entries.values()) {
                snapshot.add(entry.snapshot());
            }
            return List.copyOf(snapshot);
        }
    }

    /**
     * Stops a download and leaves it queued behind whatever else is waiting.
     *
     * <p>Idempotent, and a no-op on anything terminal: pausing a download that
     * already finished is a request that has been overtaken, not an error.
     */
    void pause(TransferId id) {
        Entry entry = entry(id);
        if (entry == null || entry.isTerminal() || !entry.paused.compareAndSet(false, true)) {
            return;
        }
        TransferState previous = entry.state;
        entry.cancellation.get().cancel();
        entry.cancellation.set(new CancellationController());
        transition(entry, new TransferState.Paused(new TransferState.Queued(0)));
        if (previous instanceof TransferState.Queued) {
            return;
        }
        admit();
    }

    /** Puts a paused download back in the queue. Idempotent. */
    void resume(TransferId id) {
        Entry entry = entry(id);
        if (entry == null || entry.isTerminal() || !entry.paused.compareAndSet(true, false)) {
            return;
        }
        transition(entry, new TransferState.Queued(0));
        admit();
    }

    /** Stops a download for good. Idempotent, and a no-op once terminal. */
    void cancel(TransferId id) {
        Entry entry = entry(id);
        if (entry == null || entry.isTerminal()) {
            return;
        }
        entry.cancellation.get().cancel();
        finish(entry, new TransferOutcome.Cancelled());
    }

    /**
     * Puts a finished download back in the queue, attempt count reset.
     *
     * <p>A no-op on anything not finished: retrying something already running is
     * a request that has been overtaken.
     */
    void retry(TransferId id) {
        Entry entry = entry(id);
        if (entry == null || !entry.isTerminal()) {
            return;
        }
        entry.attempt = 0;
        entry.endedAt = null;
        // An explicit retry is a fresh download: a consumer asking for one after
        // a rejection is not asking to resume a transfer the peer refused.
        entry.resumeOffset = 0;
        entry.paused.set(false);
        entry.cancellation.set(new CancellationController());
        transition(entry, new TransferState.Queued(0));
        admit();
    }

    /** Drops a terminal download from the list. A no-op on anything else. */
    boolean forget(TransferId id) {
        Entry entry = entry(id);
        if (entry == null || !entry.isTerminal()) {
            return false;
        }
        synchronized (lock) {
            entries.remove(id);
        }
        store.delete(id);
        return true;
    }

    /** Moves a download up or down the queue. Takes effect at the next admission. */
    void prioritize(TransferId id, Priority priority) {
        Entry entry = entry(id);
        if (entry == null) {
            return;
        }
        entry.priority = priority;
        save(entry);
        admit();
    }

    /** Closes the queue, cancelling everything still in flight. */
    void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        java.util.concurrent.ScheduledFuture<?> existing = poll;
        if (existing != null) {
            existing.cancel(false);
        }
        List<Entry> live;
        synchronized (lock) {
            live = new ArrayList<>(entries.values());
        }
        for (Entry entry : live) {
            if (!entry.isTerminal()) {
                entry.cancellation.get().cancel();
            }
        }
    }

    // --- scheduling --------------------------------------------------------

    /**
     * Starts whatever the policy now allows to start.
     *
     * <p>Ordering is by priority and then by arrival: two downloads of the same
     * priority run in the order they were asked for, which is the only ordering
     * a consumer can predict.
     */
    private void admit() {
        List<Entry> starting = new ArrayList<>();
        synchronized (lock) {
            if (closed.get()) {
                return;
            }
            int running = 0;
            Map<Username, Integer> perUser = new LinkedHashMap<>();
            for (Entry entry : entries.values()) {
                if (entry.isRunning()) {
                    running++;
                    perUser.merge(entry.user(), 1, Integer::sum);
                }
            }

            List<Entry> waiting = new ArrayList<>();
            for (Entry entry : entries.values()) {
                if (entry.state instanceof TransferState.Queued && !entry.paused.get()) {
                    waiting.add(entry);
                }
            }
            waiting.sort(Comparator.comparingInt(entry -> -entry.priority.ordinal()));

            DownloadPolicy current = policy;
            for (Entry entry : waiting) {
                if (running >= current.maxConcurrent()) {
                    break;
                }
                int mine = perUser.getOrDefault(entry.user(), 0);
                if (mine >= current.maxConcurrentPerUser()) {
                    continue;
                }
                perUser.put(entry.user(), mine + 1);
                running++;
                entry.state = new TransferState.Requesting();
                starting.add(entry);
            }

            // Local positions are what a consumer renders while waiting, and
            // they are only meaningful relative to the rest of the queue.
            renumber();
        }

        for (Entry entry : starting) {
            entry.startedAt = Instant.now();
            entry.attempt++;
            onStateChanged.accept(entry, new TransferState.Queued(0));
            save(entry);
            NetworkExecutor.runAsync(() -> attempt(entry));
        }
    }

    /** Recomputes the queue positions of everything still waiting. Under the lock. */
    private void renumber() {
        List<Entry> waiting = new ArrayList<>();
        for (Entry entry : entries.values()) {
            if (entry.state instanceof TransferState.Queued) {
                waiting.add(entry);
            }
        }
        waiting.sort(Comparator.comparingInt(entry -> -entry.priority.ordinal()));
        for (int position = 0; position < waiting.size(); position++) {
            waiting.get(position).state = new TransferState.Queued(position);
        }
    }

    private void attempt(Entry entry) {
        TransferOutcome outcome;
        try {
            outcome = Objects.requireNonNull(runner.run(entry), "runner outcome");
        } catch (RuntimeException failure) {
            outcome = new TransferOutcome.Failed(failure, true);
        }

        if (entry.paused.get()) {
            // The pause already published its own state; a cancelled attempt
            // coming back is not an outcome anyone asked to hear about.
            admit();
            return;
        }

        DownloadPolicy current = policy;
        if (current.retry().shouldRetry(outcome, entry.attempt)) {
            scheduleRetry(entry, current.retry().backoffBefore(entry.attempt + 1));
            admit();
            return;
        }
        finish(entry, outcome);
        admit();
    }

    private void scheduleRetry(Entry entry, Duration backoff) {
        transition(entry, new TransferState.Queued(0));
        long millis = Math.max(1, backoff.toMillis());
        onRetryScheduled.accept(entry, Instant.now().plusMillis(millis));
        scheduler.schedule(
                () -> {
                    if (!closed.get() && entry.state instanceof TransferState.Queued && !entry.paused.get()) {
                        admit();
                    }
                },
                millis,
                TimeUnit.MILLISECONDS);
    }

    private void finish(Entry entry, TransferOutcome outcome) {
        entry.endedAt = Instant.now();
        transition(entry, new TransferState.Finished(outcome));
    }

    /** Publishes a state change and records it. */
    private void transition(Entry entry, TransferState next) {
        TransferState previous;
        synchronized (lock) {
            previous = entry.state;
            entry.state = next;
            renumber();
        }
        save(entry);
        onStateChanged.accept(entry, previous);
    }

    /** Records an entry's current state, in state order. */
    private void save(Entry entry) {
        synchronized (storeLock) {
            store.save(entry.snapshot());
        }
    }

    private Entry entry(TransferId id) {
        Objects.requireNonNull(id, "id");
        synchronized (lock) {
            return entries.get(id);
        }
    }
}
