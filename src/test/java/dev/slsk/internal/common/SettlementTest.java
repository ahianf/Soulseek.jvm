// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The property that justifies the cell: three racing parties, one answer.
 *
 * <p>This is what {@code CompletableFuture.anyOf} over three futures used to
 * provide in the transfer path, and it is the only thing about it that was
 * load-bearing.
 */
class SettlementTest {

    @Test
    @DisplayName("a new settlement is unsettled and has no failure")
    void startsUnsettled() {
        Settlement settlement = new Settlement();

        assertFalse(settlement.isSettled());
        assertNull(settlement.failure());
    }

    @Test
    @DisplayName("the first to settle wins and everybody after it is a no-op")
    void firstSettlementWins() {
        IllegalStateException first = new IllegalStateException("first");
        Settlement settlement = new Settlement();

        assertTrue(settlement.fail(first));
        assertFalse(settlement.fail(new IllegalStateException("second")));
        assertFalse(settlement.succeed());
        assertSame(first, settlement.failure());
        assertSame(first, settlement.await());
    }

    @Test
    @DisplayName("a settlement that succeeded reports no failure")
    void successHasNoFailure() {
        Settlement settlement = new Settlement();

        assertTrue(settlement.succeed());
        assertTrue(settlement.isSettled());
        assertNull(settlement.await());
    }

    @Test
    @DisplayName("a waiter is released by whichever party settles first")
    void aWaiterIsReleasedByTheWinner() throws InterruptedException {
        Settlement settlement = new Settlement();
        IllegalStateException dropped = new IllegalStateException("the connection dropped");
        AtomicReference<Throwable> observed = new AtomicReference<>();
        CountDownLatch waiting = new CountDownLatch(1);

        try (ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor()) {
            threads.execute(() -> {
                waiting.countDown();
                observed.set(settlement.await());
            });
            assertTrue(waiting.await(5, TimeUnit.SECONDS));
            settlement.fail(dropped);
            threads.shutdown();
            assertTrue(threads.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertSame(dropped, observed.get());
    }

    @Test
    @DisplayName("exactly one of many concurrent settlements is the winner")
    void exactlyOneConcurrentSettlementWins() throws InterruptedException {
        Settlement settlement = new Settlement();
        CountDownLatch start = new CountDownLatch(1);
        List<Throwable> arms = List.of(
                new IllegalStateException("read"),
                new IllegalStateException("disconnect"),
                new IllegalStateException("remote"));
        CountDownLatch won = new CountDownLatch(1);

        try (ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Throwable arm : arms) {
                threads.execute(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (settlement.fail(arm)) {
                        won.countDown();
                    }
                });
            }
            start.countDown();
            threads.shutdown();
            assertTrue(threads.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertTrue(won.await(0, TimeUnit.SECONDS), "exactly one arm reports that it settled the race");
        assertTrue(arms.contains(settlement.failure()));
    }

    @Test
    @DisplayName("a timed wait gives up on a settlement that never comes")
    void aTimedWaitGivesUp() {
        Settlement settlement = new Settlement();

        assertFalse(settlement.await(1));
        assertFalse(settlement.isSettled());
    }

    @Test
    @DisplayName("a timed wait returns as soon as it is settled")
    void aTimedWaitSeesASettlement() {
        Settlement settlement = new Settlement();
        settlement.succeed();

        assertTrue(settlement.await(60_000));
    }
}
