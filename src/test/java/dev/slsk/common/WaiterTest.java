// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.common;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationToken;
import dev.slsk.CancellationTokenSource;
import dev.slsk.exceptions.SoulseekClientException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class WaiterTest {
    @Test
    @DisplayName("Constructors retain default timeouts")
    void constructorsRetainDefaultTimeouts() {
        try (Waiter defaultWaiter = new Waiter();
                Waiter customWaiter = new Waiter(42)) {
            assertEquals(Waiter.DEFAULT_TIMEOUT, defaultWaiter.getDefaultTimeout());
            assertEquals(42, customWaiter.getDefaultTimeout());
        }
    }

    @Test
    @DisplayName("Complete dequeues and completes the oldest wait")
    void completeDequeuesAndCompletesOldestWait() {
        try (Waiter waiter = new Waiter()) {
            WaitKey key = new WaitKey("login");
            CompletableFuture<String> first = waiter.waitAsync(key, String.class);
            CompletableFuture<String> second = waiter.waitAsync(key, String.class);

            waiter.complete(key, "first");

            assertEquals("first", first.join());
            assertFalse(second.isDone());
            assertEquals(1, waiter.getWaitCount(key));
            assertTrue(waiter.hasWait(key));

            waiter.complete(key, "second");

            assertEquals("second", second.join());
            assertFalse(waiter.hasWait(key));
            assertEquals(0, waiter.getKeyCount());
        }
    }

    @Test
    @DisplayName("Non-generic complete returns null")
    void nonGenericCompleteReturnsNull() {
        try (Waiter waiter = new Waiter()) {
            WaitKey key = new WaitKey("room-list");
            CompletableFuture<Void> wait = waiter.waitAsync(key);

            waiter.complete(key);

            assertEquals(null, wait.join());
        }
    }

    @Test
    @DisplayName("Disposition of a missing wait does not throw")
    void missingWaitDoesNotThrow() {
        try (Waiter waiter = new Waiter()) {
            WaitKey key = new WaitKey("missing");

            assertDoesNotThrow(() -> waiter.complete(key));
            assertDoesNotThrow(() -> waiter.cancel(key));
            assertDoesNotThrow(() -> waiter.timeout(key));
            assertDoesNotThrow(() -> waiter.fail(key, new RuntimeException("failure")));
        }
    }

    @Test
    @DisplayName("Cancel dequeues and cancels the oldest wait")
    void cancelDequeuesAndCancelsOldestWait() {
        try (Waiter waiter = new Waiter()) {
            WaitKey key = new WaitKey("login");
            CompletableFuture<Void> wait = waiter.waitAsync(key);

            waiter.cancel(key);

            assertThrows(CancellationException.class, wait::join);
            assertFalse(waiter.hasWait(key));
        }
    }

    @Test
    @DisplayName("Manual timeout dequeues with TimeoutException")
    void manualTimeoutDequeuesWithTimeoutException() {
        try (Waiter waiter = new Waiter()) {
            WaitKey key = new WaitKey("login");
            CompletableFuture<Void> wait = waiter.waitAsync(key);

            waiter.timeout(key);

            CompletionException exception = assertThrows(CompletionException.class, wait::join);
            assertTrue(exception.getCause() instanceof TimeoutException);
            assertFalse(waiter.hasWait(key));
        }
    }

    @Test
    @DisplayName("Automatic timeout dequeues only its oldest wait")
    void automaticTimeoutDequeuesOnlyOldestWait() {
        try (Waiter waiter = new Waiter(0)) {
            WaitKey key = new WaitKey("login");
            CompletableFuture<String> first = waiter.waitAsync(key, String.class);
            CompletableFuture<String> second = waiter.waitAsync(key, String.class, 30_000);

            CompletionException exception = assertThrows(CompletionException.class, first::join);

            assertTrue(exception.getCause() instanceof TimeoutException);
            assertFalse(second.isDone());
            assertEquals(1, waiter.getWaitCount(key));
        }
    }

    @Test
    @DisplayName("Caller cancellation dequeues a wait")
    void callerCancellationDequeuesWait() {
        try (Waiter waiter = new Waiter();
                CancellationTokenSource source = new CancellationTokenSource()) {
            WaitKey key = new WaitKey("login");
            CompletableFuture<String> wait = waiter.waitAsync(key, String.class, 30_000, source.getToken());

            source.cancel();

            assertThrows(CancellationException.class, wait::join);
            assertFalse(waiter.hasWait(key));
        }
    }

    @Test
    @DisplayName("Pre-cancelled token is handled after enqueue")
    void preCancelledTokenIsHandledAfterEnqueue() {
        try (Waiter waiter = new Waiter();
                CancellationTokenSource source = new CancellationTokenSource()) {
            source.cancel();
            WaitKey key = new WaitKey("login");

            CompletableFuture<String> wait = waiter.waitAsync(key, String.class, 30_000, source.getToken());

            assertThrows(CancellationException.class, wait::join);
            assertFalse(waiter.hasWait(key));
        }
    }

    @Test
    @DisplayName("Cancellation callback preserves source FIFO disposition")
    void cancellationCallbackPreservesFifoDisposition() {
        try (Waiter waiter = new Waiter();
                CancellationTokenSource secondSource = new CancellationTokenSource()) {
            WaitKey key = new WaitKey("login");
            CompletableFuture<String> first = waiter.waitAsync(key, String.class, 30_000);
            CompletableFuture<String> second = waiter.waitAsync(key, String.class, 30_000, secondSource.getToken());

            secondSource.cancel();

            assertThrows(CancellationException.class, first::join);
            assertFalse(second.isDone());
            assertEquals(1, waiter.getWaitCount(key));
        }
    }

    @Test
    @DisplayName("Returned future cancellation removes that exact wait")
    void returnedFutureCancellationRemovesExactWait() {
        try (Waiter waiter = new Waiter()) {
            WaitKey key = new WaitKey("login");
            CompletableFuture<String> first = waiter.waitAsync(key, String.class, 30_000);
            CompletableFuture<String> second = waiter.waitAsync(key, String.class, 30_000);

            assertTrue(second.cancel(false));

            assertFalse(first.isDone());
            assertEquals(1, waiter.getWaitCount(key));
            waiter.complete(key, "result");
            assertEquals("result", first.join());
        }
    }

    @Test
    @DisplayName("Fail preserves the supplied exception")
    void failPreservesSuppliedException() {
        try (Waiter waiter = new Waiter()) {
            WaitKey key = new WaitKey("login");
            CompletableFuture<String> wait = waiter.waitAsync(key, String.class);
            RuntimeException failure = new RuntimeException("failure");

            waiter.fail(key, failure);

            CompletionException thrown = assertThrows(CompletionException.class, wait::join);
            assertSame(failure, thrown.getCause());
        }
    }

    @Test
    @DisplayName("Type mismatch throws SoulseekClientException")
    void typeMismatchThrowsSoulseekClientException() {
        try (Waiter waiter = new Waiter()) {
            WaitKey key = new WaitKey("login");
            CompletableFuture<String> wait = waiter.waitAsync(key, String.class);

            SoulseekClientException exception =
                    assertThrows(SoulseekClientException.class, () -> waiter.complete(key, 42));

            assertTrue(exception.getMessage().contains("mismatch"));
            assertFalse(wait.isDone());
            assertFalse(waiter.hasWait(key));
        }
    }

    @Test
    @DisplayName("WaitIndefinitely uses Integer.MAX_VALUE timeout")
    void waitIndefinitelyUsesMaximumTimeout() {
        try (Waiter waiter = new Waiter()) {
            WaitKey key = new WaitKey("transfer");
            CompletableFuture<String> wait = waiter.waitIndefinitelyAsync(key, String.class);

            assertFalse(wait.isDone());
            assertEquals(1, waiter.getWaitCount(key));
        }
    }

    @Test
    @DisplayName("Minus one timeout does not schedule expiration")
    void minusOneTimeoutDoesNotScheduleExpiration() {
        try (Waiter waiter = new Waiter()) {
            WaitKey key = new WaitKey("transfer");
            CompletableFuture<String> wait = waiter.waitAsync(key, String.class, -1);

            assertFalse(wait.orTimeout(20, TimeUnit.MILLISECONDS)
                    .handle((result, exception) -> exception == null)
                    .join());
            assertThrows(IllegalArgumentException.class, () -> waiter.waitAsync(key, String.class, -2));
        }
    }

    @Test
    @DisplayName("CancelAll cancels duplicate-key waits")
    void cancelAllCancelsDuplicateKeyWaits() {
        try (Waiter waiter = new Waiter()) {
            WaitKey key = new WaitKey("login");
            CompletableFuture<String> first = waiter.waitAsync(key, String.class, 30_000);
            CompletableFuture<String> second = waiter.waitAsync(key, String.class, 30_000);

            waiter.cancelAll();

            assertTrue(first.isCancelled());
            assertTrue(second.isCancelled());
            assertEquals(0, waiter.getKeyCount());
        }
    }

    @Test
    @DisplayName("Close is idempotent, releases waits, and rejects new waits")
    void closeReleasesWaitsAndRejectsNewWaits() {
        Waiter waiter = new Waiter();
        CompletableFuture<String> pending = waiter.waitAsync(new WaitKey("login"), String.class, 30_000);

        waiter.close();
        waiter.close();

        assertTrue(pending.isCancelled());
        CompletionException exception = assertThrows(
                CompletionException.class,
                () -> waiter.waitAsync(new WaitKey("new"), String.class).join());
        assertTrue(exception.getCause() instanceof IllegalStateException);
    }

    @Test
    @DisplayName("PendingWait close works before and after registration")
    void pendingWaitCloseWorksBeforeAndAfterRegistration() {
        try (ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()) {
            Waiter.PendingWait<String> unregistered =
                    new Waiter.PendingWait<>(String.class, 30_000, () -> {}, () -> {}, CancellationToken.none());
            Waiter.PendingWait<String> registered =
                    new Waiter.PendingWait<>(String.class, 30_000, () -> {}, () -> {}, CancellationToken.none());

            assertDoesNotThrow(unregistered::close);
            registered.register(scheduler);
            assertDoesNotThrow(registered::close);
        }
    }

    @Test
    @DisplayName("Concurrent enqueue and complete does not discard waits")
    void concurrentEnqueueAndCompleteDoesNotDiscardWaits() throws InterruptedException {
        int count = 500;
        try (Waiter waiter = new Waiter()) {
            WaitKey key = new WaitKey("concurrent");
            List<CompletableFuture<Integer>> futures = Collections.synchronizedList(new ArrayList<>());

            Thread producer = Thread.ofPlatform().start(() -> {
                for (int index = 0; index < count; index++) {
                    futures.add(waiter.waitIndefinitelyAsync(key, Integer.class));
                }
            });
            Thread consumer = Thread.ofPlatform().start(() -> {
                for (int index = 0; index < count; index++) {
                    while (!waiter.hasWait(key)) {
                        Thread.onSpinWait();
                    }
                    waiter.complete(key, index);
                }
            });

            producer.join();
            consumer.join();

            assertEquals(count, futures.size());
            for (CompletableFuture<Integer> future : futures) {
                assertTrue(future.isDone());
                assertFalse(future.isCompletedExceptionally());
            }
            assertEquals(0, waiter.getKeyCount());
        }
    }
}
