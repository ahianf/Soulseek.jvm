// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationController;
import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.SoulseekClientException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class WaiterTest {
    @Test
    @DisplayName("Constructors retain default timeouts")
    void constructorsRetainDefaultTimeouts() {
        try (DefaultWaiter defaultWaiter = new DefaultWaiter();
                DefaultWaiter customWaiter = new DefaultWaiter(42)) {
            assertEquals(DefaultWaiter.DEFAULT_TIMEOUT, defaultWaiter.getDefaultTimeout());
            assertEquals(42, customWaiter.getDefaultTimeout());
        }
    }

    @Test
    @DisplayName("Complete dequeues and completes the oldest wait")
    void completeDequeuesAndCompletesOldestWait() {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey("login");
            Wait<String> first = waiter.register(key, String.class, null, null);
            Wait<String> second = waiter.register(key, String.class, null, null);

            waiter.complete(key, "first");

            assertEquals("first", first.await());
            assertEquals(1, waiter.getWaitCount(key));
            assertTrue(waiter.hasWait(key));

            waiter.complete(key, "second");

            assertEquals("second", second.await());
            assertFalse(waiter.hasWait(key));
            assertEquals(0, waiter.getKeyCount());
        }
    }

    @Test
    @DisplayName("Non-generic complete returns null")
    void nonGenericCompleteReturnsNull() {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey("room-list");
            Wait<Void> wait = waiter.register(key, null, null);

            waiter.complete(key);

            assertNull(wait.await());
        }
    }

    @Test
    @DisplayName("Disposition of a missing wait does not throw")
    void missingWaitDoesNotThrow() {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
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
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey("login");
            Wait<Void> wait = waiter.register(key, null, null);

            waiter.cancel(key);

            assertThrows(CancellationException.class, wait::await);
            assertFalse(waiter.hasWait(key));
        }
    }

    @Test
    @DisplayName("Cancelling by key takes exactly one wait, oldest first")
    void cancelTakesExactlyOneWaitOldestFirst() {
        // A caller can no longer cancel its own wait — a handle is not a
        // future — so cancellation is by key, and the queue behind that key has
        // to survive it.
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey("login");
            Wait<String> first = waiter.register(key, String.class, 30_000, null);
            Wait<String> second = waiter.register(key, String.class, 30_000, null);

            waiter.cancel(key);

            assertThrows(CancellationException.class, first::await);
            assertEquals(1, waiter.getWaitCount(key));
            waiter.complete(key, "result");
            assertEquals("result", second.await());
        }
    }

    @Test
    @DisplayName("Manual timeout dequeues with TimeoutException")
    void manualTimeoutDequeuesWithTimeoutException() {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey("login");
            Wait<Void> wait = waiter.register(key, null, null);

            waiter.timeout(key);

            CompletionException exception = assertThrows(CompletionException.class, wait::await);
            assertTrue(exception.getCause() instanceof TimeoutException);
            assertFalse(waiter.hasWait(key));
        }
    }

    @Test
    @DisplayName("Automatic timeout dequeues only its oldest wait")
    void automaticTimeoutDequeuesOnlyOldestWait() {
        try (DefaultWaiter waiter = new DefaultWaiter(0)) {
            WaitKey key = new WaitKey("login");
            Wait<String> first = waiter.register(key, String.class, null, null);
            waiter.register(key, String.class, 30_000, null);

            CompletionException exception = assertThrows(CompletionException.class, first::await);

            assertTrue(exception.getCause() instanceof TimeoutException);
            assertEquals(1, waiter.getWaitCount(key));
        }
    }

    @Test
    @DisplayName("Caller cancellation dequeues a wait")
    void callerCancellationDequeuesWait() {
        try (DefaultWaiter waiter = new DefaultWaiter();
                CancellationController source = new CancellationController()) {
            WaitKey key = new WaitKey("login");
            Wait<String> wait = waiter.register(key, String.class, 30_000, source.getSignal());

            source.cancel();

            assertThrows(CancellationException.class, wait::await);
            assertFalse(waiter.hasWait(key));
        }
    }

    @Test
    @DisplayName("Pre-cancelled token is handled after enqueue")
    void preCancelledTokenIsHandledAfterEnqueue() {
        try (DefaultWaiter waiter = new DefaultWaiter();
                CancellationController source = new CancellationController()) {
            source.cancel();
            WaitKey key = new WaitKey("login");

            Wait<String> wait = waiter.register(key, String.class, 30_000, source.getSignal());

            assertThrows(CancellationException.class, wait::await);
            assertFalse(waiter.hasWait(key));
        }
    }

    @Test
    @DisplayName("Cancellation callback preserves source FIFO disposition")
    void cancellationCallbackPreservesFifoDisposition() {
        try (DefaultWaiter waiter = new DefaultWaiter();
                CancellationController secondSource = new CancellationController()) {
            WaitKey key = new WaitKey("login");
            Wait<String> first = waiter.register(key, String.class, 30_000, null);
            waiter.register(key, String.class, 30_000, secondSource.getSignal());

            secondSource.cancel();

            assertThrows(CancellationException.class, first::await);
            assertEquals(1, waiter.getWaitCount(key));
        }
    }

    @Test
    @DisplayName("Fail preserves the supplied exception")
    void failPreservesSuppliedException() {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey("login");
            Wait<String> wait = waiter.register(key, String.class, null, null);
            RuntimeException failure = new RuntimeException("failure");

            waiter.fail(key, failure);

            CompletionException thrown = assertThrows(CompletionException.class, wait::await);
            assertSame(failure, thrown.getCause());
        }
    }

    @Test
    @DisplayName("Type mismatch throws SoulseekClientException")
    void typeMismatchThrowsSoulseekClientException() {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey("login");
            waiter.register(key, String.class, null, null);

            SoulseekClientException exception =
                    assertThrows(SoulseekClientException.class, () -> waiter.complete(key, 42));

            assertTrue(exception.getMessage().contains("mismatch"));
            assertFalse(waiter.hasWait(key));
        }
    }

    @Test
    @DisplayName("registerIndefinitely uses Integer.MAX_VALUE timeout")
    void registerIndefinitelyUsesMaximumTimeout() {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey("transfer");
            Wait<String> wait = waiter.registerIndefinitely(key, String.class, null);

            assertFalse(settles(wait));
            assertEquals(1, waiter.getWaitCount(key));
        }
    }

    @Test
    @DisplayName("Minus one timeout does not schedule expiration")
    void minusOneTimeoutDoesNotScheduleExpiration() {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey("transfer");
            Wait<String> wait = waiter.register(key, String.class, -1, null);

            assertFalse(settles(wait));
            assertThrows(IllegalArgumentException.class, () -> waiter.register(key, String.class, -2, null));
        }
    }

    @Test
    @DisplayName("CancelAll cancels duplicate-key waits")
    void cancelAllCancelsDuplicateKeyWaits() {
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey("login");
            Wait<String> first = waiter.register(key, String.class, 30_000, null);
            Wait<String> second = waiter.register(key, String.class, 30_000, null);

            waiter.cancelAll();

            assertThrows(CancellationException.class, first::await);
            assertThrows(CancellationException.class, second::await);
            assertEquals(0, waiter.getKeyCount());
        }
    }

    @Test
    @DisplayName("Close is idempotent, releases waits, and rejects new waits")
    void closeReleasesWaitsAndRejectsNewWaits() {
        DefaultWaiter waiter = new DefaultWaiter();
        Wait<String> pending = waiter.register(new WaitKey("login"), String.class, 30_000, null);

        waiter.close();
        waiter.close();

        assertThrows(CancellationException.class, pending::await);
        CompletionException exception = assertThrows(
                CompletionException.class,
                () -> waiter.register(new WaitKey("new"), String.class, null, null)
                        .await());
        assertTrue(exception.getCause() instanceof IllegalStateException);
    }

    @Test
    @DisplayName("PendingWait close works before and after registration")
    void pendingWaitCloseWorksBeforeAndAfterRegistration() {
        try (Scheduler scheduler = new Scheduler("waiter-test-timer")) {
            DefaultWaiter.PendingWait<String> unregistered = new DefaultWaiter.PendingWait<>(
                    String.class, 30_000, () -> {}, () -> {}, CancellationSignal.none());
            DefaultWaiter.PendingWait<String> registered = new DefaultWaiter.PendingWait<>(
                    String.class, 30_000, () -> {}, () -> {}, CancellationSignal.none());

            assertDoesNotThrow(unregistered::close);
            registered.register(scheduler);
            assertDoesNotThrow(registered::close);
        }
    }

    @Test
    @DisplayName("Concurrent enqueue and complete does not discard waits")
    void concurrentEnqueueAndCompleteDoesNotDiscardWaits() throws InterruptedException {
        int count = 500;
        try (DefaultWaiter waiter = new DefaultWaiter()) {
            WaitKey key = new WaitKey("concurrent");
            List<Wait<Integer>> waits = Collections.synchronizedList(new ArrayList<>());

            Thread producer = Thread.ofPlatform().start(() -> {
                for (int index = 0; index < count; index++) {
                    waits.add(waiter.registerIndefinitely(key, Integer.class, null));
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

            assertEquals(count, waits.size());
            for (Wait<Integer> wait : waits) {
                assertDoesNotThrow(wait::await);
            }
            assertEquals(0, waiter.getKeyCount());
        }
    }

    /**
     * Whether a wait settles within a short window.
     *
     * <p>A handle has no {@code isDone}, which is the point: the only way to
     * observe one is to wait on it. Asserting the negative is safe on a slow
     * machine — a machine too busy to run this in time is also too busy to
     * settle a wait nothing has answered.
     */
    private static boolean settles(Wait<?> wait) {
        CountDownLatch settled = new CountDownLatch(1);
        AtomicReference<Thread> waiter = new AtomicReference<>();
        waiter.set(Thread.ofVirtual().start(() -> {
            try {
                wait.await();
            } catch (Throwable ignored) {
                // Settling exceptionally is still settling.
            }
            settled.countDown();
        }));
        try {
            return settled.await(50, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }
}
