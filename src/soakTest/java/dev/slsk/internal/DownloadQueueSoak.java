// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.download.DownloadPolicy;
import dev.slsk.download.DownloadRequest;
import dev.slsk.internal.common.Scheduler;
import dev.slsk.soak.HeapProbe;
import dev.slsk.soak.SoakReport;
import dev.slsk.spi.TransferSink;
import dev.slsk.spi.TransferStore;
import dev.slsk.transfer.RejectionReason;
import dev.slsk.transfer.RetryPolicy;
import dev.slsk.transfer.TransferId;
import dev.slsk.transfer.TransferOutcome;
import dev.slsk.transfer.TransferState;
import dev.slsk.user.Username;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Scenario: the managed download queue under load, and under refusal.
 *
 * <p>No network, and deliberately so. The queue's whole job is announcing what
 * we want to peers and retaining it until it settles; the two ways that job goes
 * wrong at scale are serialising an album behind transfer caps that do not apply
 * yet and a retry loop that will not drain, and neither involves a socket.
 *
 * <p>Driving the queue directly, with every runner held at one gate, makes the
 * concurrency exact rather than sampled. Transfer caps are acquired later by
 * {@code DownloadRun}, when a peer actually offers the file and a transfer
 * connection exists; applying them here would put every track after the first
 * at the back of the peer's queue.
 */
class DownloadQueueSoak {

    private static final int DOWNLOADS = 200;
    private static final int PEERS = 20;
    private static final int TRANSFER_MAX_CONCURRENT = 8;
    private static final int TRANSFER_MAX_PER_USER = 2;

    /** A sink that goes nowhere. This scenario is about scheduling, not bytes. */
    private static final class NullSink implements TransferSink {
        @Override
        public WritableByteChannel open(long resumeOffset) {
            return Channels.newChannel(OutputStream.nullOutputStream());
        }

        @Override
        public void commit() {}

        @Override
        public void discard() {}
    }

    /**
     * Assigns downloads to peers in blocks rather than round-robin.
     *
     * <p>Ten consecutive tracks from one peer is what queueing an album looks
     * like. All ten must be announced together; serialising them behind the
     * transfer-stage per-user cap would make each rejoin the back of the peer's
     * queue after the previous track completed.
     */
    private static DownloadRequest request(int index, int total, int peers) {
        int peer = Math.min(peers - 1, index / Math.max(1, total / peers));
        return DownloadRequest.of(Username.of("peer-" + peer), "music\\track-" + index + ".mp3", new NullSink());
    }

    private static long liveCount(DownloadQueue queue, List<TransferId> ids) {
        return ids.stream()
                .map(queue::find)
                .filter(found -> found.isEmpty() || !found.get().isTerminal())
                .count();
    }

    private static void awaitDrained(DownloadQueue queue, List<TransferId> ids, Duration limit) {
        long deadline = System.nanoTime() + limit.toNanos();
        while (System.nanoTime() < deadline) {
            if (liveCount(queue, ids) == 0) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while draining", interrupted);
            }
        }
        throw new AssertionError(
                "the queue did not drain: " + liveCount(queue, ids) + " of " + ids.size() + " still live");
    }

    /**
     * Two hundred downloads across twenty peers are all announced at once.
     *
     * <p>The configured transfer caps are deliberately much smaller. They do
     * not apply while the files are merely taking places in remote queues; the
     * transfer stage enforces them after a peer offers a file.
     */
    @Test
    @DisplayName("Queue under sustained load")
    void queueUnderSustainedLoad() throws Exception {
        long heapBefore = HeapProbe.liveHeapBytes();

        AtomicInteger concurrent = new AtomicInteger();
        AtomicInteger peakConcurrent = new AtomicInteger();
        Map<Username, AtomicInteger> perUser = new ConcurrentHashMap<>();
        Map<Username, AtomicInteger> peakPerUser = new ConcurrentHashMap<>();
        AtomicLong runs = new AtomicLong();
        CountDownLatch allStarted = new CountDownLatch(DOWNLOADS);
        CountDownLatch release = new CountDownLatch(1);

        Scheduler scheduler = new Scheduler("download-queue-soak");
        DownloadQueue queue = new DownloadQueue(
                scheduler,
                entry -> {
                    peakConcurrent.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
                    AtomicInteger mine = perUser.computeIfAbsent(entry.user(), user -> new AtomicInteger());
                    peakPerUser
                            .computeIfAbsent(entry.user(), user -> new AtomicInteger())
                            .accumulateAndGet(mine.incrementAndGet(), Math::max);
                    allStarted.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    } finally {
                        mine.decrementAndGet();
                        concurrent.decrementAndGet();
                        runs.incrementAndGet();
                    }
                    return new TransferOutcome.Succeeded(1024, Duration.ofMillis(2));
                },
                TransferStore.inMemory(),
                (entry, previous) -> {});
        queue.policy(DownloadPolicy.defaults()
                .maxConcurrent(TRANSFER_MAX_CONCURRENT)
                .maxConcurrentPerUser(TRANSFER_MAX_PER_USER)
                .retry(RetryPolicy.none()));

        try {
            List<TransferId> ids = new ArrayList<>(DOWNLOADS);
            for (int index = 0; index < DOWNLOADS; index++) {
                TransferId id = TransferId.of("soak-" + index);
                ids.add(id);
                queue.enqueue(id, request(index, DOWNLOADS, PEERS));
            }

            assertTrue(allStarted.await(30, TimeUnit.SECONDS), "not every queued file was announced");
            release.countDown();
            awaitDrained(queue, ids, Duration.ofSeconds(120));

            int worstPerUser = peakPerUser.values().stream()
                    .mapToInt(AtomicInteger::get)
                    .max()
                    .orElse(0);
            SoakReport.record("download-queue", "downloads", DOWNLOADS);
            SoakReport.record("download-queue", "peers", PEERS);
            SoakReport.record("download-queue", "attempts run", runs.get());
            SoakReport.record("download-queue", "peak queue requests overall", peakConcurrent.get());
            SoakReport.record("download-queue", "peak queue requests per peer", worstPerUser);

            assertEquals(
                    DOWNLOADS,
                    peakConcurrent.get(),
                    "every queued file should be announced without waiting for a transfer slot");
            assertEquals(
                    DOWNLOADS / PEERS,
                    worstPerUser,
                    "every track from one peer should take its remote queue place together");
            assertEquals(DOWNLOADS, runs.get(), "every download ran exactly once");
            assertEquals(DOWNLOADS, queue.all().size(), "nothing was lost");
        } finally {
            release.countDown();
            queue.close();
            scheduler.close();
        }

        long heapAfter = HeapProbe.liveHeapBytes();
        SoakReport.record("download-queue", "live heap before", HeapProbe.formatBytes(heapBefore));
        SoakReport.record("download-queue", "live heap after", HeapProbe.formatBytes(heapAfter));
        SoakReport.record(
                "download-queue",
                "heap per download",
                String.format(Locale.ROOT, "%.0f bytes", (heapAfter - heapBefore) / (double) DOWNLOADS));
    }

    /**
     * A peer that refuses everything, with a policy that says to wait it out.
     *
     * <p>The failure this guards against is a queue that retries forever: a
     * backoff that resets, an attempt counter that does not increment, or a
     * completion path that requeues without counting. All three look identical
     * from outside — the queue simply never drains — so the assertion is that it
     * does, and that it took exactly the attempts the policy allows.
     */
    @Test
    @DisplayName("Retry storm")
    void retryStorm() {
        int downloads = 60;
        int maxAttempts = 4;
        Map<TransferId, AtomicInteger> attempts = new ConcurrentHashMap<>();
        AtomicInteger scheduled = new AtomicInteger();

        Scheduler scheduler = new Scheduler("retry-storm-soak");
        DownloadQueue queue = new DownloadQueue(
                scheduler,
                entry -> {
                    attempts.computeIfAbsent(entry.id(), id -> new AtomicInteger())
                            .incrementAndGet();
                    return new TransferOutcome.Rejected(RejectionReason.QUEUE_FULL, "Queue full.");
                },
                TransferStore.inMemory(),
                (entry, previous) -> {});
        queue.onRetryScheduled((entry, at) -> scheduled.incrementAndGet());
        queue.policy(DownloadPolicy.defaults()
                .maxConcurrent(4)
                .maxConcurrentPerUser(1)
                .retry(new RetryPolicy(
                        maxAttempts,
                        Duration.ofMillis(5),
                        2.0,
                        Duration.ofMillis(40),
                        java.util.Set.of(RejectionReason.QUEUE_FULL))));

        try {
            List<TransferId> ids = new ArrayList<>(downloads);
            for (int index = 0; index < downloads; index++) {
                TransferId id = TransferId.of("refused-" + index);
                ids.add(id);
                queue.enqueue(id, request(index, downloads, 4));
            }

            awaitDrained(queue, ids, Duration.ofSeconds(120));

            int worst = attempts.values().stream()
                    .mapToInt(AtomicInteger::get)
                    .max()
                    .orElse(0);
            SoakReport.record("retry-storm", "downloads", downloads);
            SoakReport.record("retry-storm", "retries scheduled", scheduled.get());
            SoakReport.record("retry-storm", "most attempts for one download", worst);

            assertEquals(maxAttempts, worst, "maxAttempts was not the ceiling it claims to be");
            assertEquals(
                    downloads * (maxAttempts - 1),
                    scheduled.get(),
                    "every download should schedule exactly its retries and no more");
            for (TransferId id : ids) {
                TransferState state = queue.find(id).orElseThrow().snapshot().state();
                assertTrue(state instanceof TransferState.Finished, id + " did not finish");
            }
        } finally {
            queue.close();
            scheduler.close();
        }
    }

    /**
     * The backoff is a property of the policy rather than of the network, so it
     * is asserted on the policy. Timing it through the queue would be asserting
     * the scheduler's punctuality, which is not what the number is for.
     */
    @Test
    @DisplayName("Retry backoff grows and is capped")
    void backoffGrowsAndIsCapped() {
        RetryPolicy policy = RetryPolicy.defaults();
        List<Long> waits = new ArrayList<>();
        for (int attempt = 1; attempt <= 8; attempt++) {
            waits.add(policy.backoffBefore(attempt).toMillis());
        }
        SoakReport.record("retry-storm", "backoff schedule (ms)", waits);

        for (int index = 2; index < waits.size(); index++) {
            assertTrue(waits.get(index) >= waits.get(index - 1), "the backoff shrank at attempt " + (index + 1));
        }
        assertTrue(waits.getLast() <= policy.maxBackoff().toMillis(), "the backoff passed its ceiling");
    }
}
