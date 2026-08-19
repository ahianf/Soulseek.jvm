// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.download.Download;
import dev.slsk.download.DownloadPolicy;
import dev.slsk.download.DownloadRequest;
import dev.slsk.internal.common.Scheduler;
import dev.slsk.spi.TransferSink;
import dev.slsk.spi.TransferStore;
import dev.slsk.transfer.Priority;
import dev.slsk.transfer.RejectionReason;
import dev.slsk.transfer.RetryPolicy;
import dev.slsk.transfer.TransferId;
import dev.slsk.transfer.TransferOutcome;
import dev.slsk.transfer.TransferState;
import dev.slsk.user.Username;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The scheduling rules, asserted without a socket.
 *
 * <p>This is what the split between deciding and doing is for. The queue decides
 * what runs; the runner here is a latch, so "never more than one download
 * against one peer" is a property that can be held still and looked at rather
 * than inferred from a soak run.
 */
class DownloadQueueTest {

    private final Scheduler scheduler = new Scheduler("download-queue-test");
    private final List<String> transitions = new CopyOnWriteArrayList<>();

    @AfterEach
    void closeScheduler() {
        scheduler.close();
    }

    private static DownloadRequest request(String user, String path) {
        return DownloadRequest.of(Username.of(user), path, new NullSink());
    }

    /** A sink that goes nowhere: these tests are about scheduling, not bytes. */
    private static final class NullSink implements TransferSink {
        @Override
        public java.nio.channels.WritableByteChannel open(long resumeOffset) {
            return java.nio.channels.Channels.newChannel(java.io.OutputStream.nullOutputStream());
        }

        @Override
        public void commit() {}

        @Override
        public void discard() {}
    }

    private DownloadQueue queue(DownloadQueue.Runner runner) {
        return new DownloadQueue(
                scheduler,
                runner,
                TransferStore.inMemory(),
                (entry, previous) -> transitions.add(
                        entry.id().value() + ":" + entry.snapshot().state()));
    }

    /** A runner that blocks until released, recording who is in flight. */
    private static final class GatedRunner implements DownloadQueue.Runner {
        /** Once open, every run — including ones that start later — goes straight through. */
        private volatile boolean open;

        private final Map<TransferId, CountDownLatch> gates = new ConcurrentHashMap<>();

        /** What a given run answers with, for driving a peer's refusal. */
        private final Map<TransferId, TransferOutcome> outcomes = new ConcurrentHashMap<>();

        private final List<TransferId> started = new CopyOnWriteArrayList<>();
        private final AtomicInteger concurrent = new AtomicInteger();
        private final AtomicInteger peakConcurrent = new AtomicInteger();
        private final Map<Username, AtomicInteger> perUser = new ConcurrentHashMap<>();
        private final Map<Username, AtomicInteger> peakPerUser = new ConcurrentHashMap<>();

        @Override
        public TransferOutcome run(DownloadQueue.Entry entry) {
            started.add(entry.id());
            peakConcurrent.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
            AtomicInteger mine = perUser.computeIfAbsent(entry.user(), user -> new AtomicInteger());
            peakPerUser
                    .computeIfAbsent(entry.user(), user -> new AtomicInteger())
                    .accumulateAndGet(mine.incrementAndGet(), Math::max);
            try {
                // Register the gate first, then re-read `open`. The other order
                // loses a run outright: this thread reads open == false,
                // releaseAll() sets it and counts down every gate it can see,
                // and only then does this thread create its gate — one nothing
                // will ever open, so the run sits here for the full fifteen
                // seconds and the download never reaches a terminal state.
                // Registering first means releaseAll() either sees the gate and
                // opens it, or set `open` before this re-reads it.
                CountDownLatch gate = gates.computeIfAbsent(entry.id(), id -> new CountDownLatch(1));
                if (!open) {
                    gate.await(15, TimeUnit.SECONDS);
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally {
                mine.decrementAndGet();
                concurrent.decrementAndGet();
            }
            return outcomes.getOrDefault(entry.id(), new TransferOutcome.Succeeded(1024, Duration.ofSeconds(1)));
        }

        /** Makes one run come back as a peer that has no room for it, then opens its gate. */
        void refuseAsFull(TransferId id) {
            outcomes.put(id, new TransferOutcome.Rejected(RejectionReason.TOO_MANY_FILES, "Too many files"));
            release(id);
        }

        void release(TransferId id) {
            gates.computeIfAbsent(id, key -> new CountDownLatch(1)).countDown();
        }

        void releaseAll() {
            open = true;
            gates.values().forEach(CountDownLatch::countDown);
        }
    }

    private static void awaitStarted(GatedRunner runner, int count) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (runner.started.size() < count && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }

    /** Waits for the store to have caught up with a state it trails. */
    private static void awaitStored(TransferStore store, Class<? extends TransferState> state) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            if (store.loadAll().stream().allMatch(saved -> state.isInstance(saved.state()))) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("the store never recorded " + state.getSimpleName());
    }

    private static void awaitTerminal(DownloadQueue queue, TransferId id) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            if (queue.find(id).map(DownloadQueue.Entry::isTerminal).orElse(false)) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError(id + " never reached a terminal state");
    }

    /**
     * The whole batch reaches the peer, not a slot's worth of it.
     *
     * <p>This is the rule the ceilings in {@link DownloadPolicy} used to break.
     * They bound transfers — connections at a peer — and a download waiting in a
     * peer's queue is not one: it is a line in that queue, sent over the message
     * connection the peer already has. Charging it a slot meant an album was
     * offered to the peer one track at a time, each track rejoining the back of
     * a queue it had already waited out, so a wait that should have been paid
     * once was paid once per track.
     */
    @Test
    @DisplayName("every file goes into the peer's queue at once, not one at a time")
    void theWholeBatchIsAnnouncedAtOnce() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);
        queue.policy(DownloadPolicy.defaults().maxConcurrent(1).maxConcurrentPerUser(1));

        List<TransferId> ids = new ArrayList<>();
        for (int index = 0; index < 6; index++) {
            TransferId id = TransferId.of("d" + index);
            ids.add(id);
            queue.enqueue(id, request("alice", "music\\" + index + ".mp3"));
        }

        // Deterministic: admit() moves every entry out of the local queue under
        // its own lock before it hands any of them to a thread, so by the time
        // the last enqueue returns there is nothing left waiting locally.
        assertTrue(
                queue.all().stream().noneMatch(download -> download.state() instanceof TransferState.Queued),
                "no file should still be waiting its turn locally");

        awaitStarted(runner, 6);
        assertEquals(6, runner.started.size(), "the peer should have been asked for all six");

        runner.releaseAll();
        ids.forEach(id -> awaitTerminal(queue, id));
        queue.close();
    }

    /**
     * Two peers are two queues, and neither one's ceiling reaches the other.
     */
    @Test
    @DisplayName("a peer's refusal narrows what we ask of that peer and no other")
    void aRefusalIsScopedToThePeerThatSentIt() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);

        List<TransferId> alice = fill(queue, runner, "alice", 6);
        runner.refuseAsFull(alice.get(0));
        awaitQueued(queue, alice.get(0));

        TransferId more = TransferId.of("alice-more");
        queue.enqueue(more, request("alice", "music\\more.mp3"));
        assertInstanceOf(
                TransferState.Queued.class,
                queue.find(more).orElseThrow().snapshot().state(),
                "alice is full, so this waits");

        TransferId bob = TransferId.of("bob-0");
        queue.enqueue(bob, request("bob", "music\\bob.mp3"));
        awaitStarted(runner, 7);
        assertTrue(runner.started.contains(bob), "bob never said anything about its queue");

        runner.releaseAll();
        queue.close();
    }

    /**
     * Priority orders what is waiting, and under the new model the only thing
     * that waits is what a peer has said it has no room for. With no contention
     * everything is asked for at once and there is nothing to order — which is
     * what a priority means in any scheduler.
     */
    @Test
    @DisplayName("higher priority is asked for first once a peer's queue has room again")
    void priorityOrdersWhatAPeersCeilingIsHolding() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);

        List<TransferId> initial = fill(queue, runner, "alice", 6);
        runner.refuseAsFull(initial.get(0));
        awaitQueued(queue, initial.get(0));

        // Five are still in alice's queue and alice will hold five, so these
        // three wait rather than being asked for.
        for (String name : List.of("normal-first", "normal-second", "urgent")) {
            queue.enqueue(TransferId.of(name), request("alice", "music\\" + name + ".mp3"));
        }
        queue.prioritize(TransferId.of("urgent"), Priority.HIGH);
        assertEquals(6, runner.started.size(), "nothing more should have been asked for");

        // One leaves alice's queue, so there is room for exactly one more.
        runner.release(initial.get(1));
        awaitStarted(runner, 7);
        assertEquals(TransferId.of("urgent"), runner.started.get(6), "the urgent one takes the room");

        runner.releaseAll();
        queue.close();
    }

    @Test
    @DisplayName("a download a peer has no room for reports where it is in the local queue")
    void heldDownloadsCarryTheirLocalPosition() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);

        List<TransferId> initial = fill(queue, runner, "alice", 6);
        runner.refuseAsFull(initial.get(0));
        awaitQueued(queue, initial.get(0));

        queue.enqueue(TransferId.of("first"), request("alice", "music\\one.mp3"));
        queue.enqueue(TransferId.of("second"), request("alice", "music\\two.mp3"));

        assertEquals(
                new TransferState.Queued(1),
                queue.find(TransferId.of("first")).orElseThrow().snapshot().state());
        assertEquals(
                new TransferState.Queued(2),
                queue.find(TransferId.of("second")).orElseThrow().snapshot().state());

        runner.releaseAll();
        queue.close();
    }

    /**
     * A refusal about the size of a peer's queue is not a failed attempt at the
     * file — the peer never looked at it — so it must not spend one of the
     * retry policy's attempts. A three-attempt policy would otherwise fail an
     * album's tail after three refusals that were only ever about timing.
     */
    @Test
    @DisplayName("a full queue costs no retry attempt")
    void aFullQueueDoesNotSpendAnAttempt() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);

        List<TransferId> initial = fill(queue, runner, "alice", 6);
        TransferId refused = initial.get(0);
        runner.refuseAsFull(refused);
        awaitQueued(queue, refused);

        assertEquals(
                1,
                queue.find(refused).orElseThrow().snapshot().attempt(),
                "the snapshot floors at one; the entry's own count went back to zero");
        assertFalse(queue.find(refused).orElseThrow().isTerminal(), "a full queue is not an ending");

        runner.releaseAll();
        queue.close();
    }

    /**
     * The refusal that has no progress to wait on.
     *
     * <p>A peer whose queue is full of other people's files refuses while
     * holding nothing of ours, so its ceiling lifts the moment it is set — there
     * is nothing of ours left in its queue for the ceiling to describe. If the
     * refused download went back with it, that peer would be asked again the
     * instant it said no, and again, for as long as its queue stayed full. It
     * waits out its own backoff instead, and only that expiring asks again.
     */
    @Test
    @DisplayName("a peer refusing with nothing else of ours queued is asked again later, not at once")
    void aRefusalWithNothingElseInFlightBacksOff() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);
        queue.policy(DownloadPolicy.defaults().queuePositionPollInterval(Duration.ofMillis(250)));

        TransferId only = TransferId.of("only");
        queue.enqueue(only, request("alice", "music\\only.mp3"));
        awaitStarted(runner, 1);
        runner.refuseAsFull(only);
        awaitQueued(queue, only);

        assertEquals(1, runner.started.size(), "the refusal must not turn straight back into another ask");

        awaitStarted(runner, 2);
        assertEquals(2, runner.started.size(), "and the backoff must expire, or the download is stranded");

        runner.releaseAll();
        queue.close();
    }

    /** Enqueues {@code count} downloads from one peer and waits for all of them to be asked for. */
    private List<TransferId> fill(DownloadQueue queue, GatedRunner runner, String user, int count) {
        List<TransferId> ids = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            TransferId id = TransferId.of(user + "-" + index);
            ids.add(id);
            queue.enqueue(id, request(user, "music\\" + user + index + ".mp3"));
        }
        awaitStarted(runner, count);
        return ids;
    }

    /** Waits for an entry the peer refused to settle back into the local queue. */
    private static void awaitQueued(DownloadQueue queue, TransferId id) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            if (queue.find(id).orElseThrow().snapshot().state() instanceof TransferState.Queued) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError(id + " never went back to the local queue");
    }

    @Test
    @DisplayName("a retryable rejection is tried again; a final one is not")
    void retriesFollowThePolicy() {
        AtomicInteger attempts = new AtomicInteger();
        DownloadQueue queue = queue(entry -> {
            attempts.incrementAndGet();
            return new TransferOutcome.Rejected(RejectionReason.QUEUE_FULL, "Queue full.");
        });
        queue.policy(DownloadPolicy.defaults()
                .retry(new RetryPolicy(
                        3,
                        Duration.ofMillis(1),
                        1.0,
                        Duration.ofMillis(1),
                        java.util.Set.of(RejectionReason.QUEUE_FULL))));

        TransferId id = TransferId.of("full");
        queue.enqueue(id, request("alice", "music\\one.mp3"));
        awaitTerminal(queue, id);
        assertEquals(3, attempts.get(), "three attempts means three, not three retries");

        AtomicInteger finalAttempts = new AtomicInteger();
        DownloadQueue finalQueue = queue(entry -> {
            finalAttempts.incrementAndGet();
            return new TransferOutcome.Rejected(RejectionReason.FILE_NOT_SHARED, "File not shared.");
        });
        TransferId notShared = TransferId.of("not-shared");
        finalQueue.enqueue(notShared, request("alice", "music\\two.mp3"));
        awaitTerminal(finalQueue, notShared);
        assertEquals(1, finalAttempts.get(), "a file that is not shared will not become shared");

        queue.close();
        finalQueue.close();
    }

    @Test
    @DisplayName("an Error out of an attempt frees the slot instead of wedging the entry")
    void anErrorInAnAttemptFreesTheSlot() {
        AtomicInteger attempts = new AtomicInteger();
        DownloadQueue queue = queue(entry -> {
            if (entry.id().value().equals("dying") && attempts.incrementAndGet() == 1) {
                throw new AssertionError("the attempt died, not failed");
            }
            return new TransferOutcome.Rejected(RejectionReason.FILE_NOT_SHARED, "File not shared.");
        });

        TransferId dying = TransferId.of("dying");
        TransferId next = TransferId.of("next");
        queue.enqueue(dying, request("alice", "music\\one.mp3"));

        // The Error must not strand the entry in Requesting with its admission
        // slot held; it settles like any other failed attempt.
        awaitTerminal(queue, dying);

        // And the queue is still admitting afterwards.
        queue.enqueue(next, request("alice", "music\\two.mp3"));
        awaitTerminal(queue, next);
        queue.close();
    }

    @Test
    @DisplayName("pause and resume are idempotent, and neither disturbs a finished download")
    void intentsAreIdempotent() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);
        queue.policy(DownloadPolicy.defaults().maxConcurrent(1).maxConcurrentPerUser(1));

        TransferId running = TransferId.of("running");
        TransferId waiting = TransferId.of("waiting");
        queue.enqueue(running, request("alice", "music\\one.mp3"));
        awaitStarted(runner, 1);
        queue.enqueue(waiting, request("alice", "music\\two.mp3"));

        queue.pause(waiting);
        queue.pause(waiting);
        assertInstanceOf(
                TransferState.Paused.class,
                queue.find(waiting).orElseThrow().snapshot().state());

        queue.resume(waiting);
        queue.resume(waiting);
        // Straight back to the peer rather than into a local queue: resuming is
        // asking again, and asking no longer waits for a slot.
        assertInstanceOf(
                TransferState.Requesting.class,
                queue.find(waiting).orElseThrow().snapshot().state());

        runner.releaseAll();
        awaitTerminal(queue, running);
        awaitTerminal(queue, waiting);

        // Every intent, twice, on something already finished.
        TransferState terminal = queue.find(running).orElseThrow().snapshot().state();
        for (int repeat = 0; repeat < 2; repeat++) {
            queue.pause(running);
            queue.resume(running);
            queue.cancel(running);
        }
        assertEquals(terminal, queue.find(running).orElseThrow().snapshot().state());
        queue.close();
    }

    @Test
    @DisplayName("a paused download does not take a slot")
    void pausingReleasesTheSlotToWhateverIsNext() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);
        queue.policy(DownloadPolicy.defaults().maxConcurrent(1).maxConcurrentPerUser(1));

        TransferId first = TransferId.of("first");
        TransferId second = TransferId.of("second");
        queue.enqueue(first, request("alice", "music\\one.mp3"));
        awaitStarted(runner, 1);
        queue.enqueue(second, request("alice", "music\\two.mp3"));

        queue.pause(first);
        awaitStarted(runner, 2);
        assertEquals(List.of(first, second), runner.started);

        runner.releaseAll();
        queue.close();
    }

    /**
     * cancel() publishes Finished(Cancelled) while the attempt is still in
     * flight. The returning attempt used to act on its own outcome anyway —
     * overwriting the cancellation with a second Finished, or, when the
     * outcome read as retryable, resurrecting the cancelled download back to
     * Queued after its terminal event.
     */
    @Test
    @DisplayName("a cancelled download is not finished again by its returning attempt")
    void aCancelledDownloadIsNotFinishedAgainByItsReturningAttempt() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);
        queue.policy(DownloadPolicy.defaults().maxConcurrent(1).maxConcurrentPerUser(1));

        TransferId one = TransferId.of("one");
        TransferId two = TransferId.of("two");
        queue.enqueue(one, request("alice", "music\\one.mp3"));
        awaitStarted(runner, 1);
        queue.enqueue(two, request("alice", "music\\two.mp3"));

        queue.cancel(one);
        runner.releaseAll();
        // The second download starts only once the first attempt has returned
        // and been through the queue's decision, so this is the ordering gate.
        awaitStarted(runner, 2);
        awaitTerminal(queue, two);

        assertEquals(
                new TransferState.Finished(new TransferOutcome.Cancelled()),
                queue.find(one).orElseThrow().snapshot().state(),
                "the returning attempt must not overwrite the cancellation");
        assertEquals(
                1,
                transitions.stream()
                        .filter(transition -> transition.startsWith("one:Finished"))
                        .count(),
                "one Finished for a cancelled download, not two");
        queue.close();
    }

    @Test
    void cancelIsTerminalAndRetryPutsItBack() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);

        TransferId id = TransferId.of("one");
        queue.enqueue(id, request("alice", "music\\one.mp3"));
        awaitStarted(runner, 1);

        queue.cancel(id);
        assertEquals(
                new TransferState.Finished(new TransferOutcome.Cancelled()),
                queue.find(id).orElseThrow().snapshot().state());

        // Retry puts it back in the queue; with nothing else running it is
        // admitted at once, so what matters is that it is no longer terminal.
        queue.retry(id);
        assertEquals(false, queue.find(id).orElseThrow().isTerminal());

        runner.releaseAll();
        queue.close();
    }

    @Test
    @DisplayName("forget drops a terminal download, and refuses a live one")
    void forgetOnlyAppliesToTerminalDownloads() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);

        TransferId id = TransferId.of("one");
        queue.enqueue(id, request("alice", "music\\one.mp3"));
        awaitStarted(runner, 1);
        assertEquals(false, queue.forget(id), "a running download is not something to forget");

        runner.releaseAll();
        awaitTerminal(queue, id);
        assertEquals(true, queue.forget(id));
        assertTrue(queue.find(id).isEmpty());
        assertEquals(List.of(), queue.all());
        queue.close();
    }

    @Test
    @DisplayName("the store sees every state a download passes through")
    void theStoreIsWrittenThrough() {
        TransferStore store = TransferStore.inMemory();
        DownloadQueue queue = new DownloadQueue(
                scheduler,
                entry -> new TransferOutcome.Succeeded(10, Duration.ofSeconds(1)),
                store,
                (entry, previous) -> {});

        TransferId id = TransferId.of("one");
        queue.enqueue(id, request("alice", "music\\one.mp3"));
        awaitTerminal(queue, id);

        // The record trails the state by one write, so waiting on the state is
        // not waiting on the store.
        awaitStored(store, TransferState.Finished.class);
        List<Download> saved = store.loadAll();
        assertEquals(1, saved.size());
        assertInstanceOf(TransferState.Finished.class, saved.getFirst().state());

        queue.forget(id);
        assertEquals(List.of(), store.loadAll(), "forgetting drops the record too");
        queue.close();
    }

    @Test
    @DisplayName("a running transfer's own states reach the snapshot")
    void observedStatesAreRecordedWhileRunning() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);

        TransferId id = TransferId.of("one");
        queue.enqueue(id, request("alice", "music\\one.mp3"));
        awaitStarted(runner, 1);

        queue.observed(id, new TransferState.QueuedRemotely(java.util.OptionalInt.of(4), java.time.Instant.now()));
        assertInstanceOf(
                TransferState.QueuedRemotely.class,
                queue.find(id).orElseThrow().snapshot().state());

        runner.releaseAll();
        awaitTerminal(queue, id);

        // A state arriving late must not resurrect something already finished.
        queue.observed(id, new TransferState.Transferring(dev.slsk.transfer.Progress.none(100)));
        assertInstanceOf(
                TransferState.Finished.class,
                queue.find(id).orElseThrow().snapshot().state());
        queue.close();
    }

    /**
     * The bug this exists for: a download that was visibly transferring bytes
     * reported zero of them for its entire life, because the engine reaches
     * {@code IN_PROGRESS} once and nothing refreshed the snapshot afterwards.
     * Every consumer that polls {@code all()} — which is the ordinary way to
     * render a transfer list — showed a live download frozen at 0%.
     */
    @Test
    @DisplayName("progress reaches the snapshot, and cannot revive a settled download")
    void progressIsRecordedWhileTransferring() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);

        TransferId id = TransferId.of("one");
        queue.enqueue(id, request("alice", "music\\one.mp3"));
        awaitStarted(runner, 1);

        // Before IN_PROGRESS there is nothing to update: progress that arrives
        // early must not invent a Transferring state.
        queue.progressed(id, dev.slsk.transfer.Progress.of(50, 100, 1_000));
        assertInstanceOf(
                TransferState.Requesting.class,
                queue.find(id).orElseThrow().snapshot().state());

        queue.observed(id, new TransferState.Transferring(dev.slsk.transfer.Progress.none(100)));
        assertEquals(
                0,
                ((TransferState.Transferring)
                                queue.find(id).orElseThrow().snapshot().state())
                        .progress()
                        .transferred());

        queue.progressed(id, dev.slsk.transfer.Progress.of(64, 100, 2_048));
        TransferState moved = queue.find(id).orElseThrow().snapshot().state();
        assertEquals(64, ((TransferState.Transferring) moved).progress().transferred());
        assertEquals(2_048.0, ((TransferState.Transferring) moved).progress().bytesPerSecond(), 0.0);

        runner.releaseAll();
        awaitTerminal(queue, id);

        // A sample still in flight when the transfer settled must not pull it
        // back out of its terminal state.
        queue.progressed(id, dev.slsk.transfer.Progress.of(99, 100, 2_048));
        assertInstanceOf(
                TransferState.Finished.class,
                queue.find(id).orElseThrow().snapshot().state());
        queue.close();
    }

    /**
     * The peer's own ceiling is the rule this deliberately breaks. That ceiling
     * is a guess read off a refusal the peer sent some time ago; an offer is the
     * peer speaking now, and it can only be offering a file it has room for. The
     * place it is offering took the length of its queue to earn, so refusing it
     * on the strength of the older, weaker evidence throws that away.
     */
    /**
     * A place-in-queue question blocks on its answer and there is no bulk form
     * of it, so the only thing stopping one unreachable peer from holding up
     * every other peer's questions is that they are asked in parallel. Walked as
     * one list, the peer answering here would have waited out the peer that
     * never answers — for the message timeout, per file it is holding.
     */
    @Test
    @DisplayName("a peer that will not answer does not delay the peers that will")
    void oneSilentPeerDoesNotHoldUpTheRest() throws InterruptedException {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);
        queue.policy(DownloadPolicy.defaults().queuePositionPollInterval(Duration.ofMillis(20)));

        CountDownLatch bobAnswered = new CountDownLatch(1);
        CountDownLatch aliceMayAnswer = new CountDownLatch(1);
        queue.onPositionChanged((entry, place) -> {
            if (entry.user().equals(Username.of("bob"))) {
                bobAnswered.countDown();
            }
        });
        queue.positionProbe(entry -> {
            if (entry.user().equals(Username.of("alice"))) {
                try {
                    aliceMayAnswer.await(15, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return java.util.OptionalInt.of(2);
        });

        TransferId alice = TransferId.of("alice-one");
        TransferId bob = TransferId.of("bob-one");
        queue.enqueue(alice, request("alice", "music\\alice.mp3"));
        queue.enqueue(bob, request("bob", "music\\bob.mp3"));
        awaitStarted(runner, 2);
        queue.observed(alice, new TransferState.QueuedRemotely(java.util.OptionalInt.empty(), java.time.Instant.now()));
        queue.observed(bob, new TransferState.QueuedRemotely(java.util.OptionalInt.empty(), java.time.Instant.now()));

        assertTrue(
                bobAnswered.await(15, TimeUnit.SECONDS),
                "bob's position should arrive while alice is still not answering");

        aliceMayAnswer.countDown();
        runner.releaseAll();
        queue.close();
    }

    /**
     * A peer volunteering a place in its queue used to be read and dropped,
     * because the only thing listening was the wait registered by whoever had
     * asked. Keeping it is what lets the poll interval be measured in minutes
     * without a consumer's positions going stale in between.
     */
    @Test
    @DisplayName("a place in queue nobody asked for is still recorded")
    void anUnsolicitedPositionIsRecorded() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);

        List<java.util.OptionalInt> published = new CopyOnWriteArrayList<>();
        queue.onPositionChanged((entry, place) -> published.add(place));

        TransferId id = TransferId.of("one");
        queue.enqueue(id, request("alice", "music\\one.mp3"));
        awaitStarted(runner, 1);
        queue.observed(id, new TransferState.QueuedRemotely(java.util.OptionalInt.empty(), java.time.Instant.now()));

        queue.positionReported(Username.of("alice"), "music\\one.mp3", 4);

        TransferState.QueuedRemotely recorded = assertInstanceOf(
                TransferState.QueuedRemotely.class,
                queue.find(id).orElseThrow().snapshot().state());
        assertEquals(java.util.OptionalInt.of(4), recorded.position());
        assertEquals(List.of(java.util.OptionalInt.of(4)), published);

        // Said twice, heard once: a position that has not moved is not news,
        // however it arrived.
        queue.positionReported(Username.of("alice"), "music\\one.mp3", 4);
        assertEquals(List.of(java.util.OptionalInt.of(4)), published);

        // And a file this peer is not holding for us is not ours to record.
        queue.positionReported(Username.of("alice"), "music\\other.mp3", 9);
        assertEquals(List.of(java.util.OptionalInt.of(4)), published);

        runner.releaseAll();
        queue.close();
    }

    @Test
    @DisplayName("a peer's offer starts its download immediately, past that peer's ceiling")
    void anOfferedDownloadIsPromotedPastTheCeiling() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);

        List<TransferId> initial = fill(queue, runner, "alice", 6);
        runner.refuseAsFull(initial.get(0));
        awaitQueued(queue, initial.get(0));

        TransferId held = TransferId.of("held");
        queue.enqueue(held, request("alice", "music\\held.mp3"));
        assertInstanceOf(
                TransferState.Queued.class,
                queue.find(held).orElseThrow().snapshot().state(),
                "alice will hold no more, so this waits");

        assertEquals(
                Optional.of(held),
                queue.promote(Username.of("alice"), "music\\held.mp3"),
                "an offer for a queued download should promote it");
        awaitStarted(runner, 7);
        assertTrue(runner.started.contains(held));

        runner.releaseAll();
        queue.close();
    }

    @Test
    @DisplayName("only a download we are actually holding can be promoted")
    void promotionIgnoresWhatIsNotQueued() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);

        TransferId id = TransferId.of("one");
        queue.enqueue(id, request("alice", "music\\one.mp3"));
        awaitStarted(runner, 1);

        // Already running: a second promotion would be a duplicate transfer.
        assertEquals(Optional.empty(), queue.promote(Username.of("alice"), "music\\one.mp3"));
        // Never asked for, and a peer must not be able to push us a file.
        assertEquals(Optional.empty(), queue.promote(Username.of("alice"), "music\\never.mp3"));
        assertEquals(Optional.empty(), queue.promote(Username.of("mallory"), "music\\one.mp3"));

        TransferId paused = TransferId.of("paused");
        queue.enqueue(paused, request("bob", "music\\paused.mp3"));
        queue.pause(paused);
        // A download the consumer stopped is not one a peer gets to restart.
        assertEquals(Optional.empty(), queue.promote(Username.of("bob"), "music\\paused.mp3"));

        runner.releaseAll();
        awaitTerminal(queue, id);
        assertTrue(queue.isComplete(Username.of("alice"), "music\\one.mp3"));
        assertTrue(!queue.isComplete(Username.of("alice"), "music\\never.mp3"));
        queue.close();
    }

    @Test
    @DisplayName("the peer's queue position is polled, and only a change is published")
    void positionsArePolledAndOnlyChangesPublished() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);
        queue.policy(DownloadPolicy.defaults().queuePositionPollInterval(Duration.ofMillis(20)));

        List<java.util.OptionalInt> published = new CopyOnWriteArrayList<>();
        queue.onPositionChanged((entry, place) -> published.add(place));

        AtomicInteger asked = new AtomicInteger();
        List<Integer> answers = List.of(7, 7, 3);
        queue.positionProbe(entry -> {
            int index = Math.min(asked.getAndIncrement(), answers.size() - 1);
            return java.util.OptionalInt.of(answers.get(index));
        });

        TransferId id = TransferId.of("one");
        queue.enqueue(id, request("alice", "music\\one.mp3"));
        awaitStarted(runner, 1);
        queue.observed(id, new TransferState.QueuedRemotely(java.util.OptionalInt.empty(), java.time.Instant.now()));

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (published.size() < 2 && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }

        // Three answers, two of them the same: a consumer hears twice, not
        // three times. A position that has not moved is not news.
        assertEquals(List.of(java.util.OptionalInt.of(7), java.util.OptionalInt.of(3)), published);

        runner.releaseAll();
        queue.close();
    }

    @Test
    @DisplayName("a peer that will not answer does not stop the others being asked")
    void aFailingProbeDoesNotStopThePoll() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);
        queue.policy(DownloadPolicy.defaults()
                .maxConcurrent(2)
                .maxConcurrentPerUser(1)
                .queuePositionPollInterval(Duration.ofMillis(20)));

        List<TransferId> polled = new CopyOnWriteArrayList<>();
        queue.positionProbe(entry -> {
            polled.add(entry.id());
            if (entry.user().equals(Username.of("alice"))) {
                throw new IllegalStateException("alice is not answering");
            }
            return java.util.OptionalInt.of(2);
        });

        TransferId alice = TransferId.of("alice-one");
        TransferId bob = TransferId.of("bob-one");
        queue.enqueue(alice, request("alice", "music\\one.mp3"));
        queue.enqueue(bob, request("bob", "music\\two.mp3"));
        awaitStarted(runner, 2);
        queue.observed(alice, new TransferState.QueuedRemotely(java.util.OptionalInt.empty(), java.time.Instant.now()));
        queue.observed(bob, new TransferState.QueuedRemotely(java.util.OptionalInt.empty(), java.time.Instant.now()));

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!polled.contains(bob) && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertTrue(polled.contains(bob), "bob was never asked");
        assertInstanceOf(
                TransferState.QueuedRemotely.class,
                queue.find(bob).orElseThrow().snapshot().state());

        runner.releaseAll();
        queue.close();
    }

    /**
     * A download interrupted at ninety percent should cost ten percent to
     * finish, not another whole file. The offset is what the last attempt wrote,
     * recorded by the thing that wrote it.
     */
    @Test
    @DisplayName("a retried attempt resumes from what the last one left behind")
    void aFailedAttemptLeavesAnOffsetForTheNext() {
        List<Long> offsets = new CopyOnWriteArrayList<>();
        AtomicInteger attempts = new AtomicInteger();
        DownloadQueue queue = queue(entry -> {
            offsets.add(entry.resumeOffset());
            if (attempts.incrementAndGet() == 1) {
                entry.resumeOffset(900);
                return new TransferOutcome.Rejected(RejectionReason.QUEUE_FULL, "Queue full.");
            }
            return new TransferOutcome.Succeeded(1000, Duration.ofSeconds(1));
        });
        queue.policy(DownloadPolicy.defaults()
                .retry(new RetryPolicy(
                        3,
                        Duration.ofMillis(1),
                        1.0,
                        Duration.ofMillis(1),
                        java.util.Set.of(RejectionReason.QUEUE_FULL))));

        TransferId id = TransferId.of("one");
        queue.enqueue(id, request("alice", "music\\one.mp3"));
        awaitTerminal(queue, id);

        assertEquals(List.of(0L, 900L), offsets);
        queue.close();
    }

    @Test
    @DisplayName("an explicit retry starts over, because a refused file is not a resume")
    void retryClearsTheOffset() {
        List<Long> offsets = new CopyOnWriteArrayList<>();
        DownloadQueue queue = queue(entry -> {
            offsets.add(entry.resumeOffset());
            entry.resumeOffset(500);
            return new TransferOutcome.Rejected(RejectionReason.FILE_NOT_SHARED, "File not shared.");
        });

        TransferId id = TransferId.of("one");
        queue.enqueue(id, request("alice", "music\\one.mp3"));
        awaitTerminal(queue, id);

        queue.retry(id);
        awaitTerminal(queue, id);

        assertEquals(List.of(0L, 0L), offsets);
        queue.close();
    }
}
