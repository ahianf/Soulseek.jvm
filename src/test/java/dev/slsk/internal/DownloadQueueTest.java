// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.Download;
import dev.slsk.DownloadPolicy;
import dev.slsk.DownloadRequest;
import dev.slsk.Priority;
import dev.slsk.RejectionReason;
import dev.slsk.RetryPolicy;
import dev.slsk.TransferId;
import dev.slsk.TransferOutcome;
import dev.slsk.TransferState;
import dev.slsk.Username;
import dev.slsk.internal.common.Scheduler;
import dev.slsk.spi.TransferSink;
import dev.slsk.spi.TransferStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
                if (!open) {
                    gates.computeIfAbsent(entry.id(), id -> new CountDownLatch(1))
                            .await(15, TimeUnit.SECONDS);
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally {
                mine.decrementAndGet();
                concurrent.decrementAndGet();
            }
            return new TransferOutcome.Succeeded(1024, Duration.ofSeconds(1));
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

    @Test
    @DisplayName("never more than maxConcurrent downloads at once")
    void theOverallCapIsNeverExceeded() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);
        queue.policy(DownloadPolicy.defaults().maxConcurrentPerUser(3).maxConcurrent(2));

        List<TransferId> ids = new ArrayList<>();
        for (int index = 0; index < 6; index++) {
            TransferId id = TransferId.of("d" + index);
            ids.add(id);
            queue.enqueue(id, request("alice", "music\\" + index + ".mp3"));
        }

        awaitStarted(runner, 2);
        runner.releaseAll();
        ids.forEach(id -> awaitTerminal(queue, id));
        assertEquals(2, runner.peakConcurrent.get());
        queue.close();
    }

    /**
     * The rule the library exists to hold. Four connections to one peer for four
     * tracks of one album is indistinguishable, from their side, from an attack.
     */
    @Test
    @DisplayName("never more than maxConcurrentPerUser against any one peer")
    void thePerUserCapIsNeverExceeded() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);
        queue.policy(DownloadPolicy.defaults().maxConcurrent(6).maxConcurrentPerUser(1));

        List<TransferId> ids = new ArrayList<>();
        for (int index = 0; index < 3; index++) {
            TransferId alice = TransferId.of("a" + index);
            TransferId bob = TransferId.of("b" + index);
            ids.add(alice);
            ids.add(bob);
            queue.enqueue(alice, request("alice", "music\\a" + index + ".mp3"));
            queue.enqueue(bob, request("bob", "music\\b" + index + ".mp3"));
        }

        awaitStarted(runner, 2);
        runner.releaseAll();
        ids.forEach(id -> awaitTerminal(queue, id));

        assertEquals(1, runner.peakPerUser.get(Username.of("alice")).get());
        assertEquals(1, runner.peakPerUser.get(Username.of("bob")).get());
        assertEquals(6, runner.started.size(), "everything eventually ran");
        queue.close();
    }

    @Test
    @DisplayName("higher priority goes first, and equal priority keeps its arrival order")
    void priorityOrdersTheQueue() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);
        queue.policy(DownloadPolicy.defaults().maxConcurrent(1).maxConcurrentPerUser(1));

        queue.enqueue(TransferId.of("running"), request("alice", "music\\running.mp3"));
        awaitStarted(runner, 1);

        queue.enqueue(TransferId.of("normal-first"), request("alice", "music\\one.mp3"));
        queue.enqueue(TransferId.of("normal-second"), request("alice", "music\\two.mp3"));
        queue.enqueue(TransferId.of("urgent"), request("alice", "music\\three.mp3"));
        queue.prioritize(TransferId.of("urgent"), Priority.HIGH);

        runner.releaseAll();
        for (String name : List.of("running", "normal-first", "normal-second", "urgent")) {
            awaitTerminal(queue, TransferId.of(name));
        }

        assertEquals(
                List.of(
                        TransferId.of("running"),
                        TransferId.of("urgent"),
                        TransferId.of("normal-first"),
                        TransferId.of("normal-second")),
                runner.started);
        queue.close();
    }

    @Test
    @DisplayName("a queued download reports where it is in the queue")
    void queuedDownloadsCarryTheirLocalPosition() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);
        queue.policy(DownloadPolicy.defaults().maxConcurrent(1).maxConcurrentPerUser(1));

        queue.enqueue(TransferId.of("running"), request("alice", "music\\running.mp3"));
        awaitStarted(runner, 1);
        queue.enqueue(TransferId.of("first"), request("alice", "music\\one.mp3"));
        queue.enqueue(TransferId.of("second"), request("alice", "music\\two.mp3"));

        assertEquals(
                new TransferState.Queued(0),
                queue.find(TransferId.of("first")).orElseThrow().snapshot().state());
        assertEquals(
                new TransferState.Queued(1),
                queue.find(TransferId.of("second")).orElseThrow().snapshot().state());

        runner.releaseAll();
        queue.close();
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
        assertInstanceOf(
                TransferState.Queued.class,
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
    @DisplayName("raising the cap starts something immediately rather than at the next completion")
    void aWiderPolicyTakesEffectAtOnce() {
        GatedRunner runner = new GatedRunner();
        DownloadQueue queue = queue(runner);
        queue.policy(DownloadPolicy.defaults().maxConcurrent(1).maxConcurrentPerUser(1));

        queue.enqueue(TransferId.of("first"), request("alice", "music\\one.mp3"));
        queue.enqueue(TransferId.of("second"), request("bob", "music\\two.mp3"));
        awaitStarted(runner, 1);
        assertEquals(1, runner.started.size());

        queue.policy(DownloadPolicy.defaults().maxConcurrent(2).maxConcurrentPerUser(1));
        awaitStarted(runner, 2);
        assertEquals(2, runner.started.size());

        runner.releaseAll();
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
        queue.observed(id, new TransferState.Transferring(dev.slsk.Progress.none(100)));
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
        queue.progressed(id, dev.slsk.Progress.of(50, 100, 1_000));
        assertInstanceOf(
                TransferState.Requesting.class,
                queue.find(id).orElseThrow().snapshot().state());

        queue.observed(id, new TransferState.Transferring(dev.slsk.Progress.none(100)));
        assertEquals(
                0,
                ((TransferState.Transferring)
                                queue.find(id).orElseThrow().snapshot().state())
                        .progress()
                        .transferred());

        queue.progressed(id, dev.slsk.Progress.of(64, 100, 2_048));
        TransferState moved = queue.find(id).orElseThrow().snapshot().state();
        assertEquals(64, ((TransferState.Transferring) moved).progress().transferred());
        assertEquals(2_048.0, ((TransferState.Transferring) moved).progress().bytesPerSecond(), 0.0);

        runner.releaseAll();
        awaitTerminal(queue, id);

        // A sample still in flight when the transfer settled must not pull it
        // back out of its terminal state.
        queue.progressed(id, dev.slsk.Progress.of(99, 100, 2_048));
        assertInstanceOf(
                TransferState.Finished.class,
                queue.find(id).orElseThrow().snapshot().state());
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
