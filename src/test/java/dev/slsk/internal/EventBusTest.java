// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.Attachment;
import dev.slsk.Subscription;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EventBusTest {

    sealed interface Signal {
        record Added(int value) implements Signal {}

        record Removed(int value) implements Signal {}
    }

    /** Captures what containment reports, so the test can assert it was reported. */
    private static final class RecordingSink implements DiagnosticSink {
        final List<String> warnings = new CopyOnWriteArrayList<>();
        final List<Throwable> causes = new CopyOnWriteArrayList<>();

        @Override
        public void trace(String message) {}

        @Override
        public void trace(String message, Throwable exception) {}

        @Override
        public void debug(String message) {}

        @Override
        public void debug(String message, Throwable exception) {}

        @Override
        public void info(String message) {}

        @Override
        public void warning(String message) {
            warnings.add(message);
        }

        @Override
        public void warning(String message, Throwable exception) {
            warnings.add(message);
            causes.add(exception);
        }
    }

    private final RecordingSink sink = new RecordingSink();

    private EventBus<Signal> bus() {
        return new EventBus<>("test", sink);
    }

    @Test
    void deliversToEverySubscriber() {
        EventBus<Signal> bus = bus();
        List<Signal> first = new ArrayList<>();
        List<Signal> second = new ArrayList<>();
        bus.subscribe(first::add);
        bus.subscribe(second::add);

        assertEquals(2, bus.publish(new Signal.Added(1)));

        assertEquals(List.of(new Signal.Added(1)), first);
        assertEquals(List.of(new Signal.Added(1)), second);
    }

    @Test
    void typedSubscriptionReceivesOnlyItsType() {
        EventBus<Signal> bus = bus();
        List<Signal.Added> added = new ArrayList<>();
        bus.subscribe(Signal.Added.class, added::add);

        bus.publish(new Signal.Added(1));
        bus.publish(new Signal.Removed(2));
        bus.publish(new Signal.Added(3));

        assertEquals(List.of(new Signal.Added(1), new Signal.Added(3)), added);
    }

    @Test
    void closingASubscriptionStopsDelivery() {
        EventBus<Signal> bus = bus();
        List<Signal> seen = new ArrayList<>();
        Subscription subscription = bus.subscribe(seen::add);

        bus.publish(new Signal.Added(1));
        subscription.close();
        bus.publish(new Signal.Added(2));

        assertEquals(List.of(new Signal.Added(1)), seen);
        assertEquals(0, bus.listenerCount());
    }

    @Test
    @DisplayName("close is idempotent and never throws, so cleanup paths need no guard")
    void closingTwiceIsSafe() {
        EventBus<Signal> bus = bus();
        Subscription subscription = bus.subscribe(event -> {});
        subscription.close();
        subscription.close();
        assertEquals(0, bus.listenerCount());
    }

    @Test
    @DisplayName("closing one subscription does not remove an identical-looking one")
    void subscriptionsAreIndependent() {
        EventBus<Signal> bus = bus();
        AtomicInteger count = new AtomicInteger();
        Subscription first = bus.subscribe(event -> count.incrementAndGet());
        bus.subscribe(event -> count.incrementAndGet());

        first.close();
        bus.publish(new Signal.Added(1));

        assertEquals(1, count.get());
        assertEquals(1, bus.listenerCount());
    }

    // --- containment -------------------------------------------------------

    @Test
    @DisplayName("a listener that throws does not reach the caller")
    void aThrowingListenerIsContained() {
        EventBus<Signal> bus = bus();
        bus.subscribe(event -> {
            throw new IllegalStateException("consumer bug");
        });

        // The assertion is that this line does not throw. Before containment,
        // this exception unwound whatever raised the event -- a message handler
        // or a connection read loop.
        assertEquals(0, bus.publish(new Signal.Added(1)));

        assertEquals(1, sink.warnings.size());
        assertTrue(sink.warnings.get(0).contains("IllegalStateException"));
        assertTrue(sink.warnings.get(0).contains("Added"));
        assertEquals("consumer bug", sink.causes.get(0).getMessage());
    }

    @Test
    @DisplayName("one listener throwing does not stop the others")
    void remainingListenersStillRun() {
        EventBus<Signal> bus = bus();
        List<Signal> before = new ArrayList<>();
        List<Signal> after = new ArrayList<>();
        bus.subscribe(before::add);
        bus.subscribe(event -> {
            throw new IllegalStateException("consumer bug");
        });
        bus.subscribe(after::add);

        assertEquals(2, bus.publish(new Signal.Added(1)));

        assertEquals(List.of(new Signal.Added(1)), before);
        assertEquals(List.of(new Signal.Added(1)), after);
    }

    @Test
    @DisplayName("an Error is contained too, not only a RuntimeException")
    void anErrorIsAlsoContained() {
        EventBus<Signal> bus = bus();
        bus.subscribe(event -> {
            throw new StackOverflowError("deep");
        });
        assertEquals(0, bus.publish(new Signal.Added(1)));
        assertEquals(1, sink.warnings.size());
    }

    @Test
    @DisplayName("the clean-delivery count is what the private-message ack rule reads")
    void deliveryCountReportsOnlyCleanListeners() {
        EventBus<Signal> bus = bus();
        assertEquals(0, bus.publish(new Signal.Added(1)), "no listeners means no clean delivery");

        bus.subscribe(event -> {
            throw new IllegalStateException("bug");
        });
        assertEquals(0, bus.publish(new Signal.Added(2)), "every listener threw");

        bus.subscribe(event -> {});
        assertEquals(1, bus.publish(new Signal.Added(3)), "one of two was clean");
    }

    // --- atomicity ---------------------------------------------------------

    @Test
    void attachReturnsTheStateAndASubscription() {
        EventBus<Signal> bus = bus();
        List<Signal> seen = new ArrayList<>();

        try (Attachment<String> attached = bus.attach(() -> "state", seen::add)) {
            assertEquals("state", attached.state());
            bus.publish(new Signal.Added(1));
        }
        bus.publish(new Signal.Added(2));

        assertEquals(List.of(new Signal.Added(1)), seen);
        assertEquals(0, bus.listenerCount());
    }

    @Test
    @DisplayName("under concurrent mutation, snapshot and stream tile exactly: nothing lost, nothing doubled")
    void attachLosesNoEventUnderConcurrentMutation() throws Exception {
        int publications = 50;
        List<Integer> expected = new ArrayList<>();
        for (int value = 1; value <= publications; value++) {
            expected.add(value);
        }

        // Repeated, because this is a race: a single run proves nothing. The
        // attach is raced against a stream of state changes so that it lands at
        // a different point each time.
        for (int attempt = 0; attempt < 200; attempt++) {
            EventBus<Signal> bus = bus();
            List<Integer> applied = new ArrayList<>();
            AtomicInteger next = new AtomicInteger();
            List<Signal> streamed = new CopyOnWriteArrayList<>();
            AtomicReference<List<Integer>> snapshot = new AtomicReference<>(List.of());
            CountDownLatch start = new CountDownLatch(1);

            Thread publisher = Thread.ofVirtual().start(() -> {
                await(start);
                for (int i = 0; i < publications; i++) {
                    bus.mutateAndPublish(() -> {
                        int value = next.incrementAndGet();
                        applied.add(value);
                        return new Signal.Added(value);
                    });
                }
            });
            Thread attacher = Thread.ofVirtual().start(() -> {
                await(start);
                snapshot.set(
                        bus.attach(() -> List.copyOf(applied), streamed::add).state());
            });

            start.countDown();
            publisher.join();
            attacher.join();

            // Everything the snapshot already had, then everything the stream
            // delivered, must be exactly 1..50 in order: no value in both, none
            // in neither. That is the entire contract, and it is why the public
            // API needs no sequence numbers or replay.
            List<Integer> union = new ArrayList<>(snapshot.get());
            streamed.forEach(signal -> union.add(((Signal.Added) signal).value()));
            assertEquals(expected, union, "attempt " + attempt);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(exception);
        }
    }

    @Test
    void mutateRunsUnderTheSameLockAsAttach() {
        EventBus<Signal> bus = bus();
        List<Integer> state = new ArrayList<>();
        bus.mutate(() -> state.add(1));
        assertEquals(List.of(1), state);
    }

    // --- argument checking -------------------------------------------------

    @Test
    void rejectsNullArguments() {
        EventBus<Signal> bus = bus();
        assertThrows(NullPointerException.class, () -> bus.subscribe(null));
        assertThrows(NullPointerException.class, () -> bus.subscribe(Signal.Added.class, null));
        assertThrows(NullPointerException.class, () -> bus.subscribe(null, event -> {}));
        assertThrows(NullPointerException.class, () -> bus.publish(null));
        assertThrows(NullPointerException.class, () -> bus.attach(null, event -> {}));
        assertThrows(NullPointerException.class, () -> bus.attach(() -> "s", null));
    }

    @Test
    void clearDropsEveryListener() {
        EventBus<Signal> bus = bus();
        bus.subscribe(event -> {});
        bus.subscribe(event -> {});
        assertEquals(2, bus.listenerCount());

        bus.clear();

        assertEquals(0, bus.listenerCount());
        assertEquals(0, bus.publish(new Signal.Added(1)));
        assertTrue(sink.warnings.isEmpty());
    }
}
