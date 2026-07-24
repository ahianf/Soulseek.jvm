// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationTokenSource;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
            assertEquals(5, bucket.getAsync(5).join());

            bucket.setCapacity(3);

            assertEquals(3, bucket.getCapacity());
            assertEquals(3, bucket.getCurrentCount());
            assertThrows(IllegalArgumentException.class, () -> bucket.setCapacity(0));
        }
    }

    @Test
    @DisplayName("GetAsync decrements by requested count")
    void getAsyncDecrementsByRequestedCount() {
        try (TokenBucket bucket = new TokenBucket(10, 10_000)) {
            assertEquals(5, bucket.getAsync(5).join());
            assertEquals(5, bucket.getCurrentCount());
        }
    }

    @Test
    @DisplayName("GetAsync limits a request to capacity")
    void getAsyncLimitsRequestToCapacity() {
        try (TokenBucket bucket = new TokenBucket(10, 10_000)) {
            assertEquals(10, bucket.getAsync(11).join());
        }
    }

    @Test
    @DisplayName("GetAsync returns available tokens")
    void getAsyncReturnsAvailableTokens() {
        try (TokenBucket bucket = new TokenBucket(10, 10_000)) {
            assertEquals(6, bucket.getAsync(6).join());
            assertEquals(4, bucket.getAsync(6).join());
        }
    }

    @Test
    @DisplayName("GetAsync waits for replenishment")
    void getAsyncWaitsForReplenishment() {
        try (TokenBucket bucket = new TokenBucket(1, 20)) {
            assertEquals(1, bucket.getAsync(1).join());
            CompletableFuture<Integer> pending = bucket.getAsync(1);

            assertFalse(pending.isDone());
            assertEquals(
                    1,
                    pending.orTimeout(Duration.ofSeconds(1).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)
                            .join());
        }
    }

    @Test
    @DisplayName("Pending requests are serviced in FIFO order")
    void pendingRequestsAreServicedInFifoOrder() {
        try (TokenBucket bucket = new TokenBucket(1, 25)) {
            bucket.getAsync(1).join();
            CompletableFuture<Integer> first = bucket.getAsync(1);
            CompletableFuture<Integer> second = bucket.getAsync(1);

            assertEquals(
                    1, first.orTimeout(1, java.util.concurrent.TimeUnit.SECONDS).join());
            assertFalse(second.isDone());
            assertEquals(
                    1,
                    second.orTimeout(1, java.util.concurrent.TimeUnit.SECONDS).join());
        }
    }

    @Test
    @DisplayName("Return ignores negatives and supports a capacity burst")
    void returnIgnoresNegativesAndSupportsCapacityBurst() {
        try (TokenBucket bucket = new TokenBucket(10, 10_000)) {
            bucket.getAsync(5).join();

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
            assertEquals(-5, bucket.getAsync(-5).join());
            assertEquals(15, bucket.getCurrentCount());
        }
    }

    @Test
    @DisplayName("Pre-cancelled requests complete with cancellation")
    void preCancelledRequestsCompleteWithCancellation() {
        try (TokenBucket bucket = new TokenBucket(1, 10_000);
                CancellationTokenSource source = new CancellationTokenSource()) {
            source.cancel();
            CompletableFuture<Integer> future = bucket.getAsync(1, source.getToken());

            assertThrows(CancellationException.class, future::join);
        }
    }

    @Test
    @DisplayName("Queued requests observe cancellation")
    void queuedRequestsObserveCancellation() {
        try (TokenBucket bucket = new TokenBucket(1, 10_000);
                CancellationTokenSource source = new CancellationTokenSource()) {
            bucket.getAsync(1).join();
            CompletableFuture<Integer> active = bucket.getAsync(1);
            CompletableFuture<Integer> queued = bucket.getAsync(1, source.getToken());

            source.cancel();

            assertThrows(CancellationException.class, queued::join);
            assertFalse(active.isDone());
        }
    }

    @Test
    @DisplayName("Cancelling a returned future removes its request")
    void cancellingReturnedFutureRemovesRequest() {
        try (TokenBucket bucket = new TokenBucket(1, 10_000)) {
            bucket.getAsync(1).join();
            CompletableFuture<Integer> first = bucket.getAsync(1);
            CompletableFuture<Integer> second = bucket.getAsync(1);

            assertTrue(first.cancel(false));
            assertTrue(first.isCancelled());
            assertFalse(second.isDone());
        }
    }

    @Test
    @DisplayName("Close is idempotent and releases pending requests")
    void closeIsIdempotentAndReleasesPendingRequests() {
        TokenBucket bucket = new TokenBucket(1, 10_000);
        bucket.getAsync(1).join();
        CompletableFuture<Integer> pending = bucket.getAsync(1);

        bucket.close();
        bucket.close();

        CompletionException pendingFailure = assertThrows(CompletionException.class, pending::join);
        assertTrue(pendingFailure.getCause() instanceof IllegalStateException);
        CompletionException newFailure =
                assertThrows(CompletionException.class, () -> bucket.getAsync(1).join());
        assertTrue(newFailure.getCause() instanceof IllegalStateException);
    }
}
