// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.concurrent.CancellationController;
import dev.slsk.internal.concurrent.CancellationSignal;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TokenBucketTest {
    @Test
    @DisplayName("Rejects invalid capacity")
    void rejectsInvalidCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new TokenBucket(0, 1_000));
        assertThrows(IllegalArgumentException.class, () -> new TokenBucket(-1, 1_000));
    }

    @Test
    @DisplayName("Rejects invalid interval")
    void rejectsInvalidInterval() {
        assertThrows(IllegalArgumentException.class, () -> new TokenBucket(1_000, 0));
        assertThrows(IllegalArgumentException.class, () -> new TokenBucket(1_000, -1));
    }

    @Test
    @DisplayName("Sets initial properties")
    void setsInitialProperties() {
        try (TokenBucket bucket = new TokenBucket(10, 10_000)) {
            assertEquals(10, bucket.getCapacity());
            assertEquals(10, bucket.getCurrentCount());
        }
    }

    @Test
    @DisplayName("SetCapacity validates, sets, and clamps")
    void setCapacityValidatesSetsAndClamps() {
        try (TokenBucket bucket = new TokenBucket(10, 10_000)) {
            assertEquals(5, bucket.get(5));

            bucket.setCapacity(3);

            assertEquals(3, bucket.getCapacity());
            assertEquals(3, bucket.getCurrentCount());
            assertThrows(IllegalArgumentException.class, () -> bucket.setCapacity(0));
        }
    }

    @Test
    @DisplayName("A grant decrements by the requested count")
    void getDecrementsByRequestedCount() {
        try (TokenBucket bucket = new TokenBucket(10, 10_000)) {
            assertEquals(5, bucket.get(5));
            assertEquals(5, bucket.getCurrentCount());
        }
    }

    @Test
    @DisplayName("A request is limited to capacity")
    void getLimitsRequestToCapacity() {
        try (TokenBucket bucket = new TokenBucket(10, 10_000)) {
            assertEquals(10, bucket.get(11));
        }
    }

    @Test
    @DisplayName("A short balance grants what there is rather than waiting for all of it")
    void getReturnsAvailableTokens() {
        try (TokenBucket bucket = new TokenBucket(10, 10_000)) {
            assertEquals(6, bucket.get(6));
            assertEquals(4, bucket.get(6));
        }
    }

    @Test
    @DisplayName("An empty bucket blocks the caller until it replenishes")
    void getWaitsForReplenishment() throws Exception {
        try (TokenBucket bucket = new TokenBucket(1, 20)) {
            assertEquals(1, bucket.get(1));

            Waiter waiter = Waiter.on(bucket, 1);
            waiter.awaitQueued(bucket);

            assertEquals(1, waiter.result(), "the caller was never replenished");
        }
    }

    @Test
    @DisplayName("Pending requests are serviced in FIFO order")
    void pendingRequestsAreServicedInFifoOrder() throws Exception {
        try (TokenBucket bucket = new TokenBucket(1, 25)) {
            bucket.get(1);

            // Queued one at a time and confirmed queued before the next starts,
            // so "first" means first in the deque rather than first to be
            // scheduled. With futures the two calls returned synchronously and
            // the order was free; two blocking callers have to be sequenced.
            Waiter first = Waiter.on(bucket, 1);
            first.awaitQueued(bucket);
            Waiter second = Waiter.on(bucket, 1);
            second.awaitQueued(bucket, 2);

            assertEquals(1, first.result());
            assertEquals(1, second.result());
            assertTrue(first.finishedAt() <= second.finishedAt(), "the second caller was served before the first");
        }
    }

    @Test
    @DisplayName("Return ignores negatives and supports a capacity burst")
    void returnIgnoresNegativesAndSupportsCapacityBurst() {
        try (TokenBucket bucket = new TokenBucket(10, 10_000)) {
            bucket.get(5);

            bucket.returnTokens(-5);
            assertEquals(5, bucket.getCurrentCount());

            bucket.returnTokens(50);
            assertEquals(15, bucket.getCurrentCount());
        }
    }

    @Test
    @DisplayName("Negative requests preserve source arithmetic")
    void negativeRequestsPreserveSourceArithmetic() {
        try (TokenBucket bucket = new TokenBucket(10, 10_000)) {
            assertEquals(-5, bucket.get(-5));
            assertEquals(15, bucket.getCurrentCount());
        }
    }

    @Test
    @DisplayName("A pre-cancelled request is refused without joining the queue")
    void preCancelledRequestsAreRefused() {
        try (TokenBucket bucket = new TokenBucket(1, 10_000);
                CancellationController source = new CancellationController()) {
            source.cancel();

            assertThrows(CancellationException.class, () -> bucket.get(1, source.getSignal()));
            assertEquals(0, bucket.getQueuedCount());
            assertEquals(1, bucket.getCurrentCount(), "a refused request must not spend tokens");
        }
    }

    @Test
    @DisplayName("A queued request observes cancellation, and the one ahead of it does not")
    void queuedRequestsObserveCancellation() throws Exception {
        try (TokenBucket bucket = new TokenBucket(1, 10_000);
                CancellationController source = new CancellationController()) {
            bucket.get(1);
            Waiter active = Waiter.on(bucket, 1);
            active.awaitQueued(bucket);
            Waiter queued = Waiter.on(bucket, 1, source.getSignal());
            queued.awaitQueued(bucket, 2);

            source.cancel();

            assertInstanceOf(CancellationException.class, queued.failure());
            assertFalse(active.isDone(), "cancelling the queued request disturbed the active one");
        }
    }

    @Test
    @DisplayName("Cancelling a queued request leaves the queue intact behind it")
    void cancellingAQueuedRequestLeavesTheQueueIntact() throws Exception {
        // A caller used to be able to cancel the future it was handed, which was
        // a second route into the queue that the bucket had to unpick for
        // itself. A blocking caller has only its signal, and this is what that
        // route has to keep doing: take one request out without stranding the
        // request behind it.
        try (TokenBucket bucket = new TokenBucket(1, 10_000);
                CancellationController source = new CancellationController()) {
            bucket.get(1);
            Waiter active = Waiter.on(bucket, 1);
            active.awaitQueued(bucket);
            Waiter doomed = Waiter.on(bucket, 1, source.getSignal());
            doomed.awaitQueued(bucket, 2);
            Waiter survivor = Waiter.on(bucket, 1);
            survivor.awaitQueued(bucket, 3);

            source.cancel();
            assertInstanceOf(CancellationException.class, doomed.failure());

            // One token at a time, because a return is clamped to capacity and
            // this bucket holds one. That also makes the order visible: the
            // first token has to reach the request that was already at the
            // head, and the second the one that was behind the cancelled one.
            bucket.returnTokens(1);
            assertEquals(1, active.result());
            bucket.returnTokens(1);
            assertEquals(1, survivor.result(), "the request behind the cancelled one was stranded");
        }
    }

    @Test
    @DisplayName("Close is idempotent and releases pending requests")
    void closeIsIdempotentAndReleasesPendingRequests() throws Exception {
        TokenBucket bucket = new TokenBucket(1, 10_000);
        bucket.get(1);
        Waiter pending = Waiter.on(bucket, 1);
        pending.awaitQueued(bucket);

        bucket.close();
        bucket.close();

        assertInstanceOf(IllegalStateException.class, pending.failure());
        assertThrows(IllegalStateException.class, () -> bucket.get(1));
    }

    @Test
    @DisplayName("A granted caller resumes without holding the bucket monitor")
    void aGrantedCallerDoesNotHoldTheBucketLock() throws Exception {
        // Defect 1.8: grants used to be completed while holding the bucket
        // monitor, so any inline continuation a caller chained onto one ran
        // under the same lock every other caller needs. There are no
        // continuations now, but the rule survives them: the thread that wakes
        // up must not be holding this lock.
        try (TokenBucket bucket = new TokenBucket(1, 25)) {
            bucket.get(1);

            Waiter waiter = Waiter.on(bucket, 1);
            waiter.awaitQueued(bucket);

            assertEquals(1, waiter.result());
            assertFalse(waiter.heldLock(), "a granted caller resumed holding the token bucket monitor");
        }
    }

    @Test
    @DisplayName("An idle bucket accrues on demand rather than on a tick")
    void idleBucketAccruesOnDemand() throws Exception {
        // Nothing replenishes the bucket in the background any more, so a
        // caller returning after a quiet spell has to earn its tokens on the
        // way in. A full interval of elapsed time is worth a whole capacity.
        try (TokenBucket bucket = new TokenBucket(10, 100)) {
            assertEquals(10, bucket.get(10));
            assertEquals(0, bucket.getCurrentCount());

            Thread.sleep(150);

            assertEquals(10, bucket.get(10));
        }
    }

    @Test
    @DisplayName("Returned tokens release a waiter without waiting for accrual")
    void returnedTokensReleaseAWaiterWithoutWaitingForAccrual() throws Exception {
        // The fixed-rate tick used to pick returned tokens up on its next pass.
        // With replenishment armed for the accrual deadline instead, returning
        // tokens has to drain the queue itself or this waiter sits for the full
        // ten-second interval behind tokens that are already in the bucket.
        try (TokenBucket bucket = new TokenBucket(1, 10_000)) {
            assertEquals(1, bucket.get(1));

            Waiter waiter = Waiter.on(bucket, 1);
            waiter.awaitQueued(bucket);

            bucket.returnTokens(1);

            assertEquals(1, waiter.result(), "Returned tokens did not release the waiter");
        }
    }

    /**
     * One blocking caller, on its own virtual thread.
     *
     * <p>A queued caller used to be a future the test could inspect. It is now a
     * parked thread, so the test needs something to hold its outcome and
     * something to tell it the caller has actually reached the queue — without
     * which every ordering assertion here would be a sleep.
     */
    private static final class Waiter {
        private final CountDownLatch done = new CountDownLatch(1);
        private final AtomicInteger granted = new AtomicInteger();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final AtomicBoolean heldLock = new AtomicBoolean();
        private final AtomicInteger finishedAt = new AtomicInteger();
        private static final AtomicInteger SEQUENCE = new AtomicInteger();

        private static Waiter on(TokenBucket bucket, int count) {
            return on(bucket, count, CancellationSignal.none());
        }

        private static Waiter on(TokenBucket bucket, int count, CancellationSignal signal) {
            Waiter waiter = new Waiter();
            Thread.ofVirtual().start(() -> {
                try {
                    waiter.granted.set(bucket.get(count, signal));
                    waiter.heldLock.set(Thread.holdsLock(bucket));
                } catch (Throwable thrown) {
                    waiter.failure.set(thrown);
                } finally {
                    waiter.finishedAt.set(SEQUENCE.incrementAndGet());
                    waiter.done.countDown();
                }
            });
            return waiter;
        }

        /** Blocks until this caller is in the bucket's queue at the given depth. */
        private void awaitQueued(TokenBucket bucket, int depth) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (bucket.getQueuedCount() < depth) {
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("the caller never reached the queue");
                }
                Thread.sleep(1);
            }
        }

        private void awaitQueued(TokenBucket bucket) throws InterruptedException {
            awaitQueued(bucket, 1);
        }

        private boolean isDone() {
            return done.getCount() == 0;
        }

        private int result() throws InterruptedException {
            settle();
            if (failure.get() != null) {
                throw new AssertionError("the caller failed instead of being granted", failure.get());
            }
            return granted.get();
        }

        private Throwable failure() throws InterruptedException {
            settle();
            return failure.get();
        }

        private int finishedAt() {
            return finishedAt.get();
        }

        private boolean heldLock() {
            return heldLock.get();
        }

        private void settle() throws InterruptedException {
            assertTrue(done.await(5, TimeUnit.SECONDS), "the caller never finished");
        }
    }
}
