// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
import java.util.concurrent.atomic.AtomicBoolean;
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

        assertEquals(2, deliver(bus, new Signal.Added(1)));

        assertEquals(List.of(new Signal.Added(1)), first);
        assertEquals(List.of(new Signal.Added(1)), second);
    }

    @Test
    void typedSubscriptionReceivesOnlyItsType() {
        EventBus<Signal> bus = bus();
        List<Signal.Added> added = new ArrayList<>();
        bus.subscribe(Signal.Added.class, added::add);

        deliver(bus, new Signal.Added(1));
        deliver(bus, new Signal.Removed(2));
        deliver(bus, new Signal.Added(3));

        assertEquals(List.of(new Signal.Added(1), new Signal.Added(3)), added);
    }

    @Test
    void closingASubscriptionStopsDelivery() {
        EventBus<Signal> bus = bus();
        List<Signal> seen = new ArrayList<>();
        Subscription subscription = bus.subscribe(seen::add);

        deliver(bus, new Signal.Added(1));
        subscription.close();
        deliver(bus, new Signal.Added(2));

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
        deliver(bus, new Signal.Added(1));

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
        assertEquals(0, deliver(bus, new Signal.Added(1)));

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

        assertEquals(2, deliver(bus, new Signal.Added(1)));

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
        assertEquals(0, deliver(bus, new Signal.Added(1)));
        assertEquals(1, sink.warnings.size());
    }

    @Test
    @DisplayName("the clean-delivery count is what the private-message ack rule reads")
    void deliveryCountReportsOnlyCleanListeners() {
        EventBus<Signal> bus = bus();
        assertEquals(0, deliver(bus, new Signal.Added(1)), "no listeners means no clean delivery");

        bus.subscribe(event -> {
            throw new IllegalStateException("bug");
        });
        assertEquals(0, deliver(bus, new Signal.Added(2)), "every listener threw");

        bus.subscribe(event -> {});
        assertEquals(1, deliver(bus, new Signal.Added(3)), "one of two was clean");
    }

    // --- atomicity ---------------------------------------------------------

    @Test
    void attachReturnsTheStateAndASubscription() {
        EventBus<Signal> bus = bus();
        List<Signal> seen = new ArrayList<>();

        try (Attachment<String> attached = bus.attach(() -> "state", seen::add)) {
            assertEquals("state", attached.state());
            deliver(bus, new Signal.Added(1));
        }
        deliver(bus, new Signal.Added(2));

        assertEquals(List.of(new Signal.Added(1)), seen);
        assertEquals(0, bus.listenerCount());
    }

    // --- delivery is off the publishing thread -----------------------------

    @Test
    @DisplayName("a listener that blocks for a second does not delay the publisher")
    void aBlockingListenerDoesNotDelayThePublisher() {
        EventBus<Signal> bus = bus();
        CountDownLatch reached = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        bus.subscribe(signal -> {
            reached.countDown();
            await(release);
        });

        long start = System.nanoTime();
        bus.publish(new Signal.Added(1));
        await(reached);
        bus.publish(new Signal.Added(2));
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertTrue(elapsedMillis < 1_000, "published twice in " + elapsedMillis + "ms behind a blocked listener");
        release.countDown();
        bus.close();
    }

    @Test
    @DisplayName("delivery does not run on the publishing thread")
    void deliveryIsNotOnThePublishingThread() {
        EventBus<Signal> bus = bus();
        AtomicReference<Thread> deliveringThread = new AtomicReference<>();
        CountDownLatch delivered = new CountDownLatch(1);
        bus.subscribe(signal -> {
            deliveringThread.set(Thread.currentThread());
            delivered.countDown();
        });

        bus.publish(new Signal.Added(1));

        await(delivered);
        assertNotEquals(Thread.currentThread(), deliveringThread.get());
        bus.close();
    }

    @Test
    @DisplayName("ordering holds per bus under concurrent publication")
    void orderingHoldsUnderConcurrentPublication() throws Exception {
        EventBus<Signal> bus = bus();
        List<Signal> seen = new CopyOnWriteArrayList<>();
        bus.subscribe(seen::add);

        // Every publisher's own events must arrive in the order it published
        // them. Across publishers the interleaving is whatever it is; within
        // one, one queue and one delivery thread is the guarantee.
        int publishers = 8;
        int each = 200;
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < publishers; p++) {
            int publisher = p;
            threads.add(Thread.ofVirtual().start(() -> {
                await(start);
                for (int i = 0; i < each; i++) {
                    bus.publish(new Signal.Added(publisher * each + i));
                }
            }));
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        deliver(bus, new Signal.Removed(0));

        assertEquals(publishers * each + 1, seen.size(), "nothing was dropped");
        for (int p = 0; p < publishers; p++) {
            int publisher = p;
            List<Integer> mine = seen.stream()
                    .filter(Signal.Added.class::isInstance)
                    .map(signal -> ((Signal.Added) signal).value())
                    .filter(value -> value / each == publisher)
                    .toList();
            List<Integer> expected = new ArrayList<>();
            for (int i = 0; i < each; i++) {
                expected.add(publisher * each + i);
            }
            assertEquals(expected, mine, "publisher " + publisher + " saw its own events out of order");
        }
        bus.close();
    }

    @Test
    @DisplayName("an overflowing queue blocks the publisher rather than dropping an event")
    void overflowBlocksRatherThanDrops() throws Exception {
        EventBus<Signal> bus = bus();
        CountDownLatch reached = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<Signal> seen = new CopyOnWriteArrayList<>();
        bus.subscribe(signal -> {
            if (reached.getCount() > 0) {
                reached.countDown();
                await(release);
            }
            seen.add(signal);
        });

        // One more than the queue holds, with the delivery thread wedged on the
        // first, so the last publisher has to wait for room.
        int overflow = 1_030;
        AtomicBoolean finished = new AtomicBoolean();
        Thread publisher = Thread.ofVirtual().start(() -> {
            for (int i = 0; i < overflow; i++) {
                bus.publish(new Signal.Added(i));
            }
            finished.set(true);
        });

        await(reached);
        assertFalse(publisher.join(java.time.Duration.ofMillis(250)), "the publisher is waiting for room");
        assertFalse(finished.get());

        release.countDown();
        publisher.join();
        deliver(bus, new Signal.Removed(0));
        assertEquals(
                overflow, seen.stream().filter(Signal.Added.class::isInstance).count(), "nothing was dropped");
        bus.close();
    }

    /**
     * After close nothing consumes the queue, so a publisher blocked on a full
     * one used to be wedged forever — the closed check happened once, before
     * the wait. Dropping is what close already documents for everything still
     * queued; the publisher must get its thread back.
     */
    @Test
    @DisplayName("close releases a publisher blocked on a full queue")
    void closeReleasesABlockedPublisher() throws Exception {
        EventBus<Signal> bus = bus();
        CountDownLatch wedged = new CountDownLatch(1);
        CountDownLatch never = new CountDownLatch(1);
        bus.subscribe(signal -> {
            wedged.countDown();
            await(never);
        });

        AtomicBoolean returned = new AtomicBoolean();
        Thread publisher = Thread.ofVirtual().start(() -> {
            // More than the queue holds, with the delivery thread wedged on
            // the first: this publisher ends up blocked waiting for room.
            for (int i = 0; i < 1_100; i++) {
                bus.publish(new Signal.Added(i));
            }
            returned.set(true);
        });
        await(wedged);
        assertFalse(publisher.join(java.time.Duration.ofMillis(250)), "the publisher is waiting for room");

        bus.close();

        assertTrue(publisher.join(java.time.Duration.ofSeconds(5)), "close left the publisher wedged");
        assertTrue(returned.get());
    }

    /**
     * A listener that publishes back into its own bus runs on the queue's only
     * consumer. If the queue is full at that moment, blocking would deadlock
     * the bus permanently and wedge every publisher behind it; the reentrant
     * publish is delivered inline instead, and nothing is dropped.
     */
    @Test
    @DisplayName("a listener flooding its own bus cannot deadlock the delivery thread")
    void aReentrantPublisherCannotDeadlockTheBus() throws Exception {
        EventBus<Signal> bus = bus();
        int flood = 1_100;
        AtomicInteger delivered = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(flood);
        bus.subscribe(signal -> {
            if (signal instanceof Signal.Removed) {
                // The trigger: flood the bus from its own delivery thread,
                // past the queue's capacity.
                for (int i = 0; i < flood; i++) {
                    bus.publish(new Signal.Added(i));
                }
            } else {
                delivered.incrementAndGet();
                done.countDown();
            }
        });

        bus.publish(new Signal.Removed(0));

        assertTrue(done.await(10, TimeUnit.SECONDS), "the bus deadlocked on its own queue");
        assertEquals(flood, delivered.get(), "nothing was dropped");
        bus.close();
    }

    @Test
    @DisplayName("close stops the delivery thread")
    void closeStopsTheDeliveryThread() {
        EventBus<Signal> bus = bus();
        CountDownLatch delivered = new CountDownLatch(1);
        AtomicReference<Thread> deliveringThread = new AtomicReference<>();
        bus.subscribe(signal -> {
            deliveringThread.set(Thread.currentThread());
            delivered.countDown();
        });
        deliver(bus, new Signal.Added(1));
        await(delivered);

        bus.close();

        assertFalse(deliveringThread.get().isAlive());
        // Publishing after close is a no-op rather than a block on a queue
        // nobody is draining.
        bus.publish(new Signal.Added(2));
        bus.close();
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
                snapshot.set(bus.attach(() -> List.copyOf(applied), signal -> {
                            if (signal instanceof Signal.Added added) {
                                streamed.add(added);
                            }
                        })
                        .state());
            });

            start.countDown();
            publisher.join();
            attacher.join();
            // Delivery is off the publishing thread now, so joining the
            // publisher only means every event is queued. One more event
            // through the same queue is the barrier: FIFO, one thread, so when
            // its continuation runs everything ahead of it has been delivered.
            deliver(bus, new Signal.Removed(0));

            // Everything the snapshot already had, then everything the stream
            // delivered, must be exactly 1..50 in order: no value in both, none
            // in neither. That is the entire contract, and it is why the public
            // API needs no sequence numbers or replay.
            List<Integer> union = new ArrayList<>(snapshot.get());
            streamed.forEach(signal -> union.add(((Signal.Added) signal).value()));
            assertEquals(expected, union, "attempt " + attempt);
            bus.close();
        }
    }

    /**
     * Publishes and waits for delivery, returning the clean-delivery count.
     *
     * <p>Delivery no longer happens on the publishing thread, so a test that
     * asserts on what listeners saw has to wait for it. The continuation is the
     * wait: it runs on the delivery thread after every listener has been given
     * the event, which is exactly the barrier a test needs and exactly what the
     * private-message acknowledgement uses in production.
     */
    private static int deliver(EventBus<Signal> bus, Signal event) {
        AtomicInteger delivered = new AtomicInteger(-1);
        CountDownLatch done = new CountDownLatch(1);
        bus.publish(event, count -> {
            delivered.set(count);
            done.countDown();
        });
        await(done);
        return delivered.get();
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
        assertEquals(0, deliver(bus, new Signal.Added(1)));
        assertTrue(sink.warnings.isEmpty());
    }
}
