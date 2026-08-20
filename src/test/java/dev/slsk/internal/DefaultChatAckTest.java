// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.events.ChatEvent;
import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.events.EventBus;
import dev.slsk.internal.events.PrivateMessageReceivedEvent;
import dev.slsk.internal.messaging.messages.AcknowledgePrivateMessageCommand;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.tcp.ConnectionState;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Acknowledge if and only if a listener took the message cleanly — and do it
 * somewhere other than the read loop that carried it.
 *
 * <p>The rule is 1.0's and does not move here. What moves is where the
 * acknowledgement runs: it used to be the statement after the listener call, on
 * the server connection's own read loop, so one private message cost a
 * consumer's handler plus a full server round trip before the next protocol
 * message could be read.
 */
class DefaultChatAckTest {

    @Test
    @DisplayName("a message a listener took cleanly is acknowledged")
    void acknowledgesACleanDelivery() {
        try (Fixture fixture = new Fixture()) {
            CountDownLatch acknowledged = new CountDownLatch(1);
            fixture.chat.events().subscribe(ChatEvent.MessageReceived.class, event -> {});
            fixture.onWrite = acknowledged::countDown;

            fixture.receive(41, "bob", "hello");

            // The acknowledgement is the continuation, so it lands after the
            // listener rather than with it; waiting on the write is what says
            // the whole rule ran.
            assertTrue(await(acknowledged));
            assertEquals(List.of(41), fixture.acknowledged());
        }
    }

    @Test
    @DisplayName("a message every listener threw on is left for the server to redeliver")
    void doesNotAcknowledgeWhenEveryListenerThrows() {
        try (Fixture fixture = new Fixture()) {
            CountDownLatch seen = new CountDownLatch(1);
            fixture.chat.events().subscribe(ChatEvent.MessageReceived.class, event -> {
                seen.countDown();
                throw new IllegalStateException("consumer bug");
            });

            fixture.receive(42, "bob", "hello");

            assertTrue(await(seen));
            fixture.settle();
            assertEquals(List.of(), fixture.acknowledged());
            assertTrue(
                    fixture.diagnostic.warnings.stream().anyMatch(warning -> warning.contains("was not acknowledged")));
        }
    }

    @Test
    @DisplayName("a message nobody is listening for is left for the server to redeliver")
    void doesNotAcknowledgeWithNoListeners() {
        try (Fixture fixture = new Fixture()) {
            fixture.receive(43, "bob", "hello");

            fixture.settle();
            assertEquals(List.of(), fixture.acknowledged());
        }
    }

    /**
     * The Definition of Done's assertion, made directly: a listener that blocks
     * for a second does not delay the next protocol message.
     *
     * <p>Both events are published from one thread, standing in for the server
     * connection's read loop. If delivery were still on that thread, the second
     * publication could not return until the first listener had finished.
     */
    @Test
    @DisplayName("a listener that blocks for a second does not delay the next message")
    void aBlockingListenerDoesNotDelayTheReadLoop() {
        try (Fixture fixture = new Fixture()) {
            CountDownLatch first = new CountDownLatch(1);
            CountDownLatch released = new CountDownLatch(1);
            fixture.chat.events().subscribe(ChatEvent.MessageReceived.class, event -> {
                first.countDown();
                await(released, 5_000);
            });

            long start = System.nanoTime();
            fixture.receive(44, "bob", "one");
            assertTrue(await(first), "the first message reached the listener");
            fixture.receive(45, "bob", "two");
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

            assertTrue(
                    elapsedMillis < 1_000,
                    "the read loop published both messages in " + elapsedMillis + "ms while a listener was blocked");
            released.countDown();
        }
    }

    @Test
    @DisplayName("the acknowledgement runs on the delivery thread, not the publishing one")
    void theAcknowledgementLeavesThePublishingThread() {
        try (Fixture fixture = new Fixture()) {
            CountDownLatch acknowledged = new CountDownLatch(1);
            fixture.chat.events().subscribe(ChatEvent.MessageReceived.class, event -> {});
            fixture.onWrite = () -> acknowledged.countDown();

            Thread publisher = Thread.currentThread();
            fixture.receive(46, "bob", "hello");

            assertTrue(await(acknowledged));
            assertNotEquals(publisher, fixture.writingThread.get());
            assertFalse(fixture.writingThread.get().isInterrupted());
        }
    }

    private static boolean await(CountDownLatch latch) {
        return await(latch, 10_000);
    }

    private static boolean await(CountDownLatch latch, long millis) {
        try {
            return latch.await(millis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** A chat facet over an engine whose server writes are recorded. */
    private static final class Fixture implements AutoCloseable {

        private final DiagnosticProbe diagnostic = new DiagnosticProbe();
        private final ConcurrentLinkedQueue<OutgoingMessage> written = new ConcurrentLinkedQueue<>();
        private final AtomicReference<Thread> writingThread = new AtomicReference<>();
        private volatile Runnable onWrite = () -> {};
        private final SoulseekEngine client;
        private final EventBus<ChatEvent> events;
        private final DefaultChat chat;

        private Fixture() {
            MessageConnection connection = (MessageConnection) Proxy.newProxyInstance(
                    MessageConnection.class.getClassLoader(),
                    new Class<?>[] {MessageConnection.class},
                    this::onConnectionCall);
            client = new SoulseekEngine(
                    9999,
                    null,
                    connection,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    diagnostic.proxy,
                    null,
                    null,
                    null);
            client.setStateForTest(SoulseekClientState.LOGGED_IN);
            events = new EventBus<>("chat", diagnostic.proxy);
            chat = new DefaultChat(client, events, diagnostic.proxy);
        }

        /** Publishes an inbound private message the way the server read loop does. */
        private void receive(int id, String from, String message) {
            client.publishEvent(
                    EngineEvents.Kind.PRIVATE_MESSAGE_RECEIVED,
                    new PrivateMessageReceivedEvent(id, Instant.now(), from, message, false));
        }

        /** Waits until the bus has delivered everything published so far. */
        private void settle() {
            CountDownLatch done = new CountDownLatch(1);
            events.publish(
                    new ChatEvent.MessageReceived(
                            dev.slsk.user.Username.of("barrier"), "", false, Instant.now(), Instant.now()),
                    delivered -> done.countDown());
            assertTrue(await(done));
        }

        private List<Integer> acknowledged() {
            return written.stream()
                    .filter(AcknowledgePrivateMessageCommand.class::isInstance)
                    .map(message -> ((AcknowledgePrivateMessageCommand) message).getId())
                    .toList();
        }

        private Object onConnectionCall(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("write")
                    && arguments != null
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage message) {
                writingThread.set(Thread.currentThread());
                written.add(message);
                onWrite.run();
                return null;
            }
            if (method.getName().equals("getState")) {
                return ConnectionState.CONNECTED;
            }
            if (method.getName().equals("getId")) {
                return UUID.randomUUID();
            }
            return defaultValue(method.getReturnType());
        }

        @Override
        public void close() {
            events.close();
            client.close();
        }
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == double.class) {
            return 0D;
        }
        return null;
    }

    private static final class DiagnosticProbe {
        private final ConcurrentLinkedQueue<String> messages = new ConcurrentLinkedQueue<>();
        private final List<String> warnings = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final DiagnosticSink proxy = (DiagnosticSink) Proxy.newProxyInstance(
                DiagnosticSink.class.getClassLoader(), new Class<?>[] {DiagnosticSink.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("warning") && arguments != null && arguments[0] instanceof String message) {
                warnings.add(message);
            } else if (arguments != null && arguments.length > 0 && arguments[0] instanceof String message) {
                messages.add(message);
            }
            return defaultValue(method.getReturnType());
        }
    }
}
