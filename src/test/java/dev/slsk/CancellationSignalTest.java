// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CancellationSignalTest {
    @Test
    @DisplayName("None is a stable non-cancellable signal")
    void noneIsStableAndNonCancellable() {
        CancellationSignal token = CancellationSignal.none();
        AtomicInteger calls = new AtomicInteger();

        try (CancellationSubscription ignored = token.register(calls::incrementAndGet)) {
            assertSame(token, CancellationSignal.none());
            assertFalse(token.isCancellationRequested());
            assertDoesNotThrow(token::throwIfCancellationRequested);
        }

        assertEquals(0, calls.get());
    }

    @Test
    @DisplayName("Cancellation is observable and idempotent")
    void cancellationIsObservableAndIdempotent() {
        try (CancellationController source = new CancellationController()) {
            CancellationSignal token = source.getSignal();
            AtomicInteger calls = new AtomicInteger();
            token.register(calls::incrementAndGet);

            source.cancel();
            source.cancel();

            assertTrue(token.isCancellationRequested());
            assertEquals(1, calls.get());
            assertThrows(CancellationException.class, token::throwIfCancellationRequested);
        }
    }

    @Test
    @DisplayName("Pre-cancellation invokes new listeners synchronously")
    void preCancellationInvokesNewListenersSynchronously() {
        try (CancellationController source = new CancellationController()) {
            source.cancel();
            AtomicInteger calls = new AtomicInteger();

            try (CancellationSubscription ignored = source.getSignal().register(calls::incrementAndGet)) {
                assertEquals(1, calls.get());
            }
        }
    }

    @Test
    @DisplayName("Closing a subscription removes its listener")
    void closingSubscriptionRemovesListener() {
        try (CancellationController source = new CancellationController()) {
            AtomicInteger calls = new AtomicInteger();
            CancellationSubscription subscription = source.getSignal().register(calls::incrementAndGet);

            subscription.close();
            subscription.close();
            source.cancel();

            assertEquals(0, calls.get());
        }
    }

    @Test
    @DisplayName("Cancellation listeners run in reverse registration order")
    void listenersRunInReverseRegistrationOrder() {
        try (CancellationController source = new CancellationController()) {
            List<Integer> order = new ArrayList<>();
            source.getSignal().register(() -> order.add(1));
            source.getSignal().register(() -> order.add(2));
            source.getSignal().register(() -> order.add(3));

            source.cancel();

            assertEquals(List.of(3, 2, 1), order);
        }
    }

    @Test
    @DisplayName("Listener failures do not prevent later listeners")
    void listenerFailuresDoNotPreventLaterListeners() {
        try (CancellationController source = new CancellationController()) {
            List<Integer> calls = new ArrayList<>();
            RuntimeException first = new RuntimeException("first");
            RuntimeException second = new RuntimeException("second");
            source.getSignal().register(() -> {
                calls.add(1);
                throw first;
            });
            source.getSignal().register(() -> {
                calls.add(2);
                throw second;
            });

            RuntimeException thrown = assertThrows(RuntimeException.class, source::cancel);

            assertSame(second, thrown);
            assertEquals(List.of(2, 1), calls);
            assertEquals(1, thrown.getSuppressed().length);
            assertSame(first, thrown.getSuppressed()[0]);
        }
    }

    @Test
    @DisplayName("Closing a source is idempotent and releases listeners")
    void closingSourceIsIdempotentAndReleasesListeners() {
        CancellationController source = new CancellationController();
        AtomicInteger calls = new AtomicInteger();
        source.getSignal().register(calls::incrementAndGet);

        source.close();
        source.close();

        assertEquals(0, calls.get());
        assertThrows(IllegalStateException.class, source::cancel);
        assertThrows(IllegalStateException.class, () -> source.getSignal().register(() -> {}));
    }

    @Test
    @DisplayName("Register and cancel race invokes a listener at most once")
    void registerAndCancelRaceInvokesListenerAtMostOnce() throws InterruptedException {
        for (int iteration = 0; iteration < 100; iteration++) {
            try (CancellationController source = new CancellationController()) {
                AtomicInteger calls = new AtomicInteger();
                CountDownLatch start = new CountDownLatch(1);
                List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

                Thread registerThread = Thread.ofPlatform().start(() -> {
                    await(start, failures);
                    source.getSignal().register(calls::incrementAndGet);
                });
                Thread cancelThread = Thread.ofPlatform().start(() -> {
                    await(start, failures);
                    source.cancel();
                });

                start.countDown();
                registerThread.join();
                cancelThread.join();

                assertEquals(List.of(), failures);
                assertEquals(1, calls.get());
            }
        }
    }

    private static void await(CountDownLatch latch, List<Throwable> failures) {
        try {
            latch.await();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            failures.add(exception);
        }
    }
}
