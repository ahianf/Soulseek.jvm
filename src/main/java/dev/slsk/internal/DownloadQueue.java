// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.CancellationController;
import dev.slsk.Download;
import dev.slsk.DownloadPolicy;
import dev.slsk.DownloadRequest;
import dev.slsk.Priority;
import dev.slsk.Progress;
import dev.slsk.TransferId;
import dev.slsk.TransferOutcome;
import dev.slsk.TransferState;
import dev.slsk.internal.common.NetworkExecutor;
import dev.slsk.internal.common.Scheduler;
import dev.slsk.spi.TransferStore;
import dev.slsk.user.Username;
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
     * The narrowest ceiling a peer's refusal may impose.
     *
     * <p>Nicotine+'s floor, and it is a floor rather than a number because a
     * peer that refuses at one file is describing a moment, not a policy:
     * dropping to one file per peer on the strength of a single refusal is the
     * behaviour this class exists to avoid, and would be entered by any peer
     * that happened to be busy when we first asked.
     */
    private static final int MINIMUM_QUEUE_LIMIT = 5;

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

        /**
         * Set when the peer refused this file because its queue for us is full.
         *
         * <p>Distinct from paused, and from a retry backoff. The download is
         * still wanted and the peer is still willing; there is simply no room
         * in its queue yet. Held entries are skipped by {@link #admit()} until
         * {@link #holdForQueueLimit} 's timer clears this — without which the
         * refused file would be asked for again immediately and refused again.
         */
        private volatile boolean queueLimited;

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

    /**
     * How many files each peer has told us it will hold for us at once.
     *
     * <p>Learned, never configured. Soulseek has no way to ask a peer how big
     * its queue is, so the only honest number is the one its refusal implies,
     * and a client that guesses low is slow for everyone while a client that
     * guesses high is refused. Absent means no peer has said, which is the
     * common case and means unbounded.
     *
     * <p>Guarded by {@link #lock}.
     */
    private final Map<Username, Integer> userQueueLimits = new LinkedHashMap<>();

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

    /** Whether a poll cycle is still in flight; see {@link #pollPositions}. */
    private final AtomicBoolean polling = new AtomicBoolean();

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
     * Starts a queued download now, because its peer says it is ready.
     *
     * <p>The one case where the queue's own ordering is the wrong answer. A peer
     * offering a file has reached our name in its queue, and that place is worth
     * more than a slot: refusing it costs however long the wait was and puts us
     * at the back. So this admits past both caps rather than joining the
     * ordinary rotation.
     *
     * <p>{@code Queued} is the state both waiting-for-a-slot and
     * serving-out-a-retry-backoff park in, so one check covers both. Paused is
     * excluded — a download the consumer stopped is not one a peer can restart.
     * A retry already scheduled for the entry becomes a no-op on its own, since
     * it re-checks the state before admitting.
     *
     * @param user who is offering
     * @param path what they are offering
     * @return the promoted download, or empty if we have nothing queued for it
     */
    Optional<TransferId> promote(Username user, String path) {
        Entry promoted = null;
        synchronized (lock) {
            if (closed.get()) {
                return Optional.empty();
            }
            for (Entry entry : entries.values()) {
                if (entry.user().equals(user)
                        && entry.request().path().equals(path)
                        && entry.state instanceof TransferState.Queued
                        && !entry.paused.get()) {
                    promoted = entry;
                    break;
                }
            }
            if (promoted == null) {
                return Optional.empty();
            }
            // A peer offering a file has room for it by definition, whatever it
            // last said about its queue being full.
            promoted.queueLimited = false;
            promoted.state = new TransferState.Requesting();
            renumber();
        }

        promoted.startedAt = Instant.now();
        promoted.attempt++;
        onStateChanged.accept(promoted, new TransferState.Queued(0));
        save(promoted);
        Entry starting = promoted;
        NetworkExecutor.executor().execute(() -> attempt(starting));
        return Optional.of(promoted.id());
    }

    /**
     * Whether a download of this file from this peer has already succeeded.
     *
     * <p>Only so an offer for it can be refused as {@code Complete} rather than
     * {@code Cancelled}; the peer uses the difference to decide whether to keep
     * the file queued for us.
     *
     * @param user the peer
     * @param path the file
     * @return whether it is already done
     */
    boolean isComplete(Username user, String path) {
        synchronized (lock) {
            for (Entry entry : entries.values()) {
                if (entry.user().equals(user)
                        && entry.request().path().equals(path)
                        && entry.state instanceof TransferState.Finished finished
                        && finished.outcome() instanceof TransferOutcome.Succeeded) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Records how far a running download has got.
     *
     * <p>Deliberately not {@link #observed}. A state change is rare, is worth a
     * store write and is worth telling a consumer about; progress is none of
     * those. The engine reaches {@code IN_PROGRESS} exactly once, so without a
     * separate path the snapshot keeps the byte count it had at that moment —
     * zero — for the whole transfer, and every consumer reading {@code all()}
     * shows a download that is running and never moves.
     *
     * <p>Applied only to a download already {@code Transferring}, so a sample
     * that arrives after the transfer settled cannot pull it back out of its
     * terminal state.
     *
     * @param id which download
     * @param progress how far it has got
     */
    void progressed(TransferId id, Progress progress) {
        Objects.requireNonNull(progress, "progress");
        Entry entry = entry(id);
        if (entry == null) {
            return;
        }
        synchronized (lock) {
            if (entry.state instanceof TransferState.Transferring) {
                entry.state = new TransferState.Transferring(progress);
            }
        }
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

    /**
     * Asks each peer where we are in its queue, one peer at a time in parallel.
     *
     * <p>Per peer rather than per file, because a place-in-queue question blocks
     * on its answer and there is no bulk form of it on the wire. Walked as one
     * list this cost the sum of every answer: a peer holding fifteen of our
     * files took fifteen round trips, and a peer that had stopped answering
     * charged the message timeout for each of its files before the next peer was
     * even asked. Fifteen silent files at a five-second timeout is seventy-five
     * seconds of a poll that was scheduled once a minute, so the polls ran
     * back-to-back with no gap between them.
     *
     * <p>Grouped by peer, a cycle costs the slowest <em>peer</em> rather than the
     * sum of all of them, and one unreachable peer no longer delays the rest. It
     * stays one outstanding question per peer, which is the part worth keeping:
     * fifteen at once would be fifteen simultaneous requests at a peer that is
     * already doing us a favour.
     *
     * <p>The guard is what makes the cost bounded rather than merely spread. A
     * cycle that outlasts its own interval skips the next tick instead of
     * overlapping with it, so a slow peer can never accumulate rounds of
     * questions nobody is waiting for.
     */
    private void pollPositions() {
        PositionProbe current = probe;
        if (current == null || closed.get() || !polling.compareAndSet(false, true)) {
            return;
        }
        Map<Username, List<Entry>> byPeer = new LinkedHashMap<>();
        synchronized (lock) {
            for (Entry entry : entries.values()) {
                if (entry.state instanceof TransferState.QueuedRemotely) {
                    byPeer.computeIfAbsent(entry.user(), user -> new ArrayList<>())
                            .add(entry);
                }
            }
        }
        if (byPeer.isEmpty()) {
            polling.set(false);
            return;
        }

        java.util.concurrent.atomic.AtomicInteger outstanding =
                new java.util.concurrent.atomic.AtomicInteger(byPeer.size());
        for (List<Entry> peerEntries : byPeer.values()) {
            NetworkExecutor.executor().execute(() -> {
                try {
                    for (Entry entry : peerEntries) {
                        if (closed.get()) {
                            return;
                        }
                        askWhereWeAre(current, entry);
                    }
                } finally {
                    if (outstanding.decrementAndGet() == 0) {
                        polling.set(false);
                    }
                }
            });
        }
    }

    /** Asks one peer about one file, and publishes the answer if it has moved. */
    private void askWhereWeAre(PositionProbe current, Entry entry) {
        java.util.OptionalInt place;
        try {
            place = current.place(entry);
        } catch (RuntimeException unreachable) {
            // A peer that will not answer about one file is not a reason to stop
            // asking about the rest, and the position we have is still the last
            // one it gave.
            return;
        }
        recordPosition(entry, place);
    }

    /**
     * Records where a peer says we are, from wherever we heard it.
     *
     * @param entry the download
     * @param place the peer's position, or empty if it did not say
     */
    private void recordPosition(Entry entry, java.util.OptionalInt place) {
        if (!(entry.state instanceof TransferState.QueuedRemotely known)
                || known.position().equals(place)) {
            return;
        }
        transition(entry, new TransferState.QueuedRemotely(place, Instant.now()));
        onPositionChanged.accept(entry, place);
    }

    /**
     * Records a place-in-queue a peer sent without being asked.
     *
     * <p>Peers volunteer these — this library's own uploader answers a
     * {@code QueueUpload} with one — and until now a place that arrived without
     * a wait registered for it was dropped on the floor. Keeping it is what lets
     * the poll interval be measured in minutes without a consumer's queue
     * positions going stale in between.
     *
     * @param user who sent it
     * @param path the file it is about
     * @param position where that peer says we are
     */
    void positionReported(Username user, String path, int position) {
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(path, "path");
        Entry reported = null;
        synchronized (lock) {
            for (Entry entry : entries.values()) {
                if (entry.user().equals(user)
                        && entry.request().path().equals(path)
                        && entry.state instanceof TransferState.QueuedRemotely) {
                    reported = entry;
                    break;
                }
            }
        }
        if (reported != null) {
            recordPosition(reported, java.util.OptionalInt.of(position));
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
        // A consumer asking for this download again is a reason to ask the peer
        // again, whatever it said about its queue while we were not asking.
        entry.queueLimited = false;
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
        entry.queueLimited = false;
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
     * Asks each peer to queue everything we want from it.
     *
     * <p>Everything, not a slot's worth. Asking is a {@code QueueUpload} on the
     * peer message connection that peer already has, so a download waiting in a
     * peer's queue costs one line in that queue and nothing else — no
     * connection, no slot, no thread that another download could have used. The
     * ceilings in {@link DownloadPolicy} bound transfers, and are taken when a
     * peer says it is ready rather than when we ask.
     *
     * <p>Charging a place in a peer's queue as though it were a transfer is what
     * this used to do, and it made {@code maxConcurrentPerUser}'s default of one
     * mean "one file per peer at a time, end to end". A fifteen-track album
     * therefore paid the peer's whole queue wait fifteen times over, rejoining
     * the back of it after every track, when one wait would have done: the point
     * of a remote queue is to hold your place in it for all of them at once.
     *
     * <p>What still bounds this is the peer itself, through
     * {@link #userQueueLimits} — a ceiling learned from its refusals rather than
     * guessed at in advance. See {@link #holdForQueueLimit}.
     *
     * <p>Ordering is by priority and then by arrival: two downloads of the same
     * priority are announced in the order they were asked for, which is the only
     * ordering a consumer can predict.
     */
    private void admit() {
        List<Entry> starting = new ArrayList<>();
        synchronized (lock) {
            if (closed.get()) {
                return;
            }
            Map<Username, Integer> inFlight = new LinkedHashMap<>();
            for (Entry entry : entries.values()) {
                if (entry.isRunning()) {
                    inFlight.merge(entry.user(), 1, Integer::sum);
                }
            }
            releaseDrainedQueueLimits(inFlight);

            List<Entry> waiting = new ArrayList<>();
            for (Entry entry : entries.values()) {
                if (entry.state instanceof TransferState.Queued && !entry.paused.get() && !entry.queueLimited) {
                    waiting.add(entry);
                }
            }
            waiting.sort(Comparator.comparingInt(entry -> -entry.priority.ordinal()));

            for (Entry entry : waiting) {
                int mine = inFlight.getOrDefault(entry.user(), 0);
                if (mine >= ceilingFor(entry.user())) {
                    continue;
                }
                inFlight.put(entry.user(), mine + 1);
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
            NetworkExecutor.executor().execute(() -> attempt(entry));
        }
    }

    /** Returns how many files a peer will hold for us, or unbounded if it has never said. */
    private int ceilingFor(Username user) {
        return userQueueLimits.getOrDefault(user, Integer.MAX_VALUE);
    }

    /**
     * Forgets the ceilings of peers whose queues we no longer occupy.
     *
     * <p>A ceiling is a fact about how full a peer's queue was when it refused
     * us, and once the last of our files has left that queue the fact has
     * expired. The peer's own progress is the signal, which is what lets a
     * ceiling lift as soon as there is room rather than when a timer says so.
     *
     * <p>Deliberately does not un-hold the download the refusal was about. That
     * one waits out {@link #holdForQueueLimit}'s timer, because a peer can
     * refuse a file when it is holding nothing else of ours — a peer whose queue
     * is full of other people's files does exactly that — and there is no
     * progress to wait for. Clearing the hold here as well would re-ask that
     * peer the instant it refused, forever. Under the lock.
     */
    private void releaseDrainedQueueLimits(Map<Username, Integer> inFlight) {
        userQueueLimits.keySet().removeIf(user -> inFlight.getOrDefault(user, 0) == 0);
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

        if (entry.isTerminal()) {
            // Same rule as paused: cancel() already published Finished while
            // this attempt was still in flight. Acting on the returning
            // outcome published a second Finished for the same entry — or,
            // when the outcome read as retryable, resurrected a CANCELLED
            // download back to Queued after its terminal event.
            admit();
            return;
        }

        if (outcome instanceof TransferOutcome.Rejected rejected && isQueueLimit(rejected.reason())) {
            holdForQueueLimit(entry);
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

    /**
     * Whether a refusal is about the size of the peer's queue rather than the
     * file.
     *
     * <p>These two are the only refusals a peer sends that will stop being true
     * on their own. Everything else — not shared, banned, cancelled — is about
     * the file or about us, and waiting does not change it.
     */
    private static boolean isQueueLimit(dev.slsk.RejectionReason reason) {
        return reason == dev.slsk.RejectionReason.TOO_MANY_FILES
                || reason == dev.slsk.RejectionReason.TOO_MANY_MEGABYTES;
    }

    /**
     * Parks a download the peer has no room for, and narrows what we ask of that
     * peer.
     *
     * <p>This is the whole of the queue's backpressure, and it is reactive on
     * purpose. A client cannot know how many files a peer will hold — the
     * protocol has no way to ask — so the alternatives are to guess low, which
     * is the slowness this class exists to remove, or to guess high and be
     * refused. Being refused is cheap and carries the answer, so the ceiling is
     * read off the refusal: {@code max(5, what the peer was already holding)},
     * the same rule and the same floor as Nicotine+'s
     * {@code _upload_denied}.
     *
     * <p>The attempt is given back. A full queue is not a failed attempt at this
     * file — the peer never looked at it — and spending one of
     * {@link dev.slsk.RetryPolicy#maxAttempts} on it would fail an album's
     * tail after three refusals that were only ever about timing.
     */
    private void holdForQueueLimit(Entry entry) {
        synchronized (lock) {
            int announced = 0;
            for (Entry other : entries.values()) {
                if (other != entry && other.user().equals(entry.user()) && other.isRunning()) {
                    announced++;
                }
            }
            userQueueLimits.put(entry.user(), Math.max(MINIMUM_QUEUE_LIMIT, announced));
            entry.queueLimited = true;
            entry.attempt = Math.max(0, entry.attempt - 1);
        }
        transition(entry, new TransferState.Queued(0));

        // The peer's queue draining lifts its ceiling, but it cannot lift this
        // download's own hold: a peer whose queue is full of other people's
        // files refuses while holding nothing of ours, so there is no progress
        // to wait on and nothing to signal. A timer is the only thing left, and
        // the poll interval is already this library's answer to "how often is it
        // worth asking a peer about its queue".
        long millis = Math.max(1, policy.queuePositionPollInterval().toMillis());
        onRetryScheduled.accept(entry, Instant.now().plusMillis(millis));
        scheduler.schedule(
                () -> {
                    if (closed.get()) {
                        return;
                    }
                    entry.queueLimited = false;
                    admit();
                },
                millis,
                TimeUnit.MILLISECONDS);
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

    /**
     * Records an entry's current state, in state order.
     *
     * <p>The record trails the state by the width of one write: a snapshot read
     * through {@code find} can show a download finished a moment before the
     * store has been told. That is the right way round for a persistence log —
     * blocking a state change on a consumer's database would make the queue only
     * as fast as its slowest writer — but it means the store is not a read
     * model, and nothing should treat it as one.
     */
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
