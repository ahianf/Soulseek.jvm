// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationController;
import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.ConnectionReadException;
import dev.slsk.exceptions.ConnectionWriteDroppedException;
import dev.slsk.exceptions.ConnectionWriteException;
import dev.slsk.internal.common.Monitors;
import dev.slsk.internal.options.ConnectionOptions;
import dev.slsk.internal.options.ProxyOptions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConnectionTest {
    private static final InetSocketAddress ENDPOINT = new InetSocketAddress(InetAddress.getLoopbackAddress(), 2234);

    @Test
    @DisplayName("SocketConnection construction preserves defaults and identity")
    void constructsWithDefaults() {
        try (SocketConnection connection = new SocketConnection(ENDPOINT, null, null, Monitors.shared())) {
            assertSame(ENDPOINT, connection.getIpEndpoint());
            assertEquals(new ConnectionKey(ENDPOINT), connection.getKey());
            assertEquals(ConnectionState.PENDING, connection.getState());
            assertEquals(ConnectionTypes.NONE, connection.getType());
            assertNotNull(connection.getId());
            assertNotEquals(new java.util.UUID(0, 0), connection.getId());
            assertTrue(connection.getInactiveTime().compareTo(Duration.ZERO) >= 0);

            ConnectionTypes type = ConnectionTypes.OUTBOUND.or(ConnectionTypes.DIRECT);
            connection.setType(type);
            assertEquals(type, connection.getType());
        }
    }

    @Test
    @DisplayName("Construction configures socket and adopted stream")
    void configuresConnectedClient() throws Exception {
        FakeStream stream = new FakeStream();
        FakeTcpClient client = new FakeTcpClient(stream, true);
        AtomicReference<Socket> configured = new AtomicReference<>();
        ConnectionOptions options = options(8, 9, 3, 100, 1_000, null, configured::set);

        try (SocketConnection connection = new SocketConnection(ENDPOINT, options, client, Monitors.shared())) {
            assertSame(client.socket, configured.get());
            // The read timeout is the cancellation poll interval, deliberately
            // decoupled from the inactivity timeout (1_000 here): the periodic
            // monitor owns inactivity, and a bounded SO_TIMEOUT is what lets a
            // read be abandoned without closing the socket. See goal 2.2.
            assertEquals(250, client.socket.getSoTimeout());
            assertEquals(250, stream.readTimeout);
            assertEquals(1_000, stream.writeTimeout);
            assertEquals(ConnectionState.CONNECTED, connection.getState());
            assertSame(options, connection.getOptions());
        }
    }

    @Test
    @DisplayName("Disconnect raises both state changes and completion")
    void disconnectRaisesEvents() throws Exception {
        FakeTcpClient client = new FakeTcpClient(new FakeStream(), true);
        SocketConnection connection = new SocketConnection(ENDPOINT, noTimers(), client, Monitors.shared());
        List<ConnectionStateChangedEvent> states = new ArrayList<>();
        List<ConnectionDisconnectedEvent> disconnected = new ArrayList<>();
        connection.addStateChangedListener((sender, args) -> {
            assertSame(connection, sender);
            states.add(args);
        });
        connection.addDisconnectedListener((sender, args) -> {
            assertSame(connection, sender);
            disconnected.add(args);
        });

        connection.disconnect("done");

        assertEquals(ConnectionState.DISCONNECTED, connection.getState());
        assertEquals(2, states.size());
        assertEquals(ConnectionState.DISCONNECTING, states.get(0).getCurrentState());
        assertEquals(ConnectionState.DISCONNECTED, states.get(1).getCurrentState());
        assertEquals("done", disconnected.get(0).getMessage());
        assertEquals("done", connection.awaitDisconnect(null));
        assertTrue(client.closed);
        connection.disconnect("ignored");
        assertEquals(2, states.size());
        connection.close();
    }

    @Test
    @DisplayName("Direct connect configures stream and raises source events")
    void connectsDirectly() throws Exception {
        FakeStream stream = new FakeStream();
        FakeTcpClient client = new FakeTcpClient(stream, false);
        client.connectAction = () -> client.connected = true;
        SocketConnection connection = new SocketConnection(ENDPOINT, noTimers(), client, Monitors.shared());
        List<ConnectionState> states = new ArrayList<>();
        AtomicInteger connected = new AtomicInteger();
        connection.addStateChangedListener((sender, args) -> states.add(args.getCurrentState()));
        connection.addConnectedListener((sender, args) -> connected.incrementAndGet());

        connection.connect(null);

        assertEquals(1, client.connectCalls);
        assertEquals(0, client.proxyCalls);
        assertEquals(List.of(ConnectionState.CONNECTING, ConnectionState.CONNECTED), states);
        assertEquals(1, connected.get());
        assertEquals(ConnectionState.CONNECTED, connection.getState());
        // Read timeout is the cancellation poll interval regardless of the
        // connection's inactivity setting (-1, disabled, for this connection).
        assertEquals(250, stream.readTimeout);
        assertEquals(-1, stream.writeTimeout);
        connection.close();
    }

    @Test
    @DisplayName("Proxy connect forwards every configured argument")
    void connectsThroughProxy() throws Exception {
        FakeStream stream = new FakeStream();
        FakeTcpClient client = new FakeTcpClient(stream, false);
        client.connectAction = () -> client.connected = true;
        ProxyOptions proxy = new ProxyOptions("127.0.0.1", 1080, "alice", "secret");
        ConnectionOptions options = options(8, 8, 3, 100, -1, proxy, null);
        SocketConnection connection = new SocketConnection(ENDPOINT, options, client, Monitors.shared());

        connection.connect(CancellationSignal.none());

        assertEquals(0, client.connectCalls);
        assertEquals(1, client.proxyCalls);
        assertEquals(proxy.getIpAddress(), client.lastProxyAddress);
        assertEquals(1080, client.lastProxyPort);
        assertEquals(ENDPOINT.getAddress(), client.lastDestination);
        assertEquals(2234, client.lastDestinationPort);
        assertEquals("alice", client.lastUsername);
        assertEquals("secret", client.lastPassword);
        connection.close();
    }

    @Test
    @DisplayName("Connect timeout disconnects and preserves TimeoutException")
    void connectTimesOut() throws Exception {
        FakeTcpClient client = new FakeTcpClient(new FakeStream(), false);
        client.connectFuture = new CompletableFuture<>();
        SocketConnection connection =
                new SocketConnection(ENDPOINT, options(8, 8, 3, 10, -1, null, null), client, Monitors.shared());

        Throwable failure = failureOf(() -> connection.connect(null));

        assertTrue(failure instanceof TimeoutException);
        assertEquals(ConnectionState.DISCONNECTED, connection.getState());
        connection.close();
    }

    @Test
    @DisplayName("Connect cancellation disconnects without remapping")
    void connectCancels() throws Exception {
        FakeTcpClient client = new FakeTcpClient(new FakeStream(), false);
        client.connectFuture = new CompletableFuture<>();
        SocketConnection connection =
                new SocketConnection(ENDPOINT, options(8, 8, 3, 5_000, -1, null, null), client, Monitors.shared());

        try (CancellationController source = new CancellationController()) {
            source.cancel();
            assertThrows(CancellationException.class, () -> connection.connect(source.getSignal()));
        }
        assertEquals(ConnectionState.DISCONNECTED, connection.getState());
        connection.close();
    }

    @Test
    @DisplayName("Connect failures are wrapped with the original cause")
    void connectWrapsFailure() throws Exception {
        FakeTcpClient client = new FakeTcpClient(new FakeStream(), false);
        IOException cause = new IOException("broken");
        client.connectFuture = CompletableFuture.failedFuture(cause);
        SocketConnection connection = new SocketConnection(ENDPOINT, noTimers(), client, Monitors.shared());

        Throwable failure = failureOf(() -> connection.connect(null));

        assertTrue(failure instanceof ConnectionException);
        assertSame(cause, failure.getCause());
        assertEquals(ConnectionState.DISCONNECTED, connection.getState());
        connection.close();
    }

    @Test
    @DisplayName("Inactivity timer and watchdog disconnect independently")
    void timersDisconnect() throws Exception {
        FakeTcpClient inactiveClient = new FakeTcpClient(new FakeStream(), true);
        SocketConnection inactive = new SocketConnection(
                ENDPOINT, options(8, 8, 3, 100, 20, null, null), inactiveClient, Monitors.shared());
        assertTrue(failureOf(() -> inactive.awaitDisconnect(null)) instanceof TimeoutException);
        inactive.close();

        FakeTcpClient watchedClient = new FakeTcpClient(new FakeStream(), true);
        SocketConnection watched = new SocketConnection(ENDPOINT, noTimers(), watchedClient, Monitors.shared());
        watchedClient.connected = false;
        Throwable watchdogFailure = failureOf(() -> watched.awaitDisconnect(null));
        assertTrue(watchdogFailure instanceof ConnectionException);
        assertEquals("The connection was closed unexpectedly", watchdogFailure.getMessage());
        watched.close();
    }

    @Test
    @DisplayName("Wait cancellation disconnects the connection")
    void waitCancellationDisconnects() {
        FakeTcpClient client = new FakeTcpClient(new FakeStream(), true);
        SocketConnection connection = new SocketConnection(ENDPOINT, noTimers(), client, Monitors.shared());
        try (CancellationController source = new CancellationController()) {
            // Already cancelled, so registering runs the callback inline: the
            // wait disconnects the connection rather than parking on it.
            source.cancel();
            assertThrows(CancellationException.class, () -> connection.awaitDisconnect(source.getSignal()));
        }
        assertEquals(ConnectionState.DISCONNECTED, connection.getState());
        connection.close();
    }

    @Test
    @DisplayName("Read loops over partial data and reports each iteration")
    void readsPartialData() throws Exception {
        FakeStream stream = new FakeStream();
        stream.readCounts.addAll(List.of(1, 2, 2));
        stream.readBytes = new byte[] {1, 2, 3, 4, 5};
        FakeTcpClient client = new FakeTcpClient(stream, true);
        SocketConnection connection =
                new SocketConnection(ENDPOINT, options(2, 2, 3, 100, -1, null, null), client, Monitors.shared());
        List<int[]> reports = new ArrayList<>();
        List<Long> progress = new ArrayList<>();
        connection.addDataReadListener((sender, args) -> {
            assertSame(connection, sender);
            progress.add(args.getCurrentLength());
        });
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        connection.read(
                5,
                output,
                (requested, token) -> requested,
                (requested, granted, transferred) -> reports.add(new int[] {requested, granted, transferred}),
                null);

        assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, output.toByteArray());
        assertEquals(List.of(1L, 3L, 5L), progress);
        assertArrayEquals(new int[] {2, 2, 1}, reports.get(0));
        assertArrayEquals(new int[] {2, 2, 2}, reports.get(1));
        assertArrayEquals(new int[] {2, 2, 2}, reports.get(2));
        assertEquals(List.of(2, 2, 2), stream.readSizes);
        connection.close();
    }

    @Test
    @DisplayName("Read array supports zero and long lengths")
    void readsArrayAndZero() throws Exception {
        FakeStream stream = new FakeStream();
        stream.readBytes = new byte[] {9, 8, 7};
        stream.readCounts.addAll(List.of(2, 1));
        SocketConnection connection = new SocketConnection(
                ENDPOINT, options(2, 2, 3, 100, -1, null, null), new FakeTcpClient(stream, true), Monitors.shared());

        assertArrayEquals(new byte[0], connection.read(0, null));
        assertArrayEquals(new byte[] {9, 8, 7}, connection.read(3L, null));
        assertThrows(IllegalArgumentException.class, () -> connection.read(-1, null));
        connection.close();
    }

    @Test
    @DisplayName("Read failures disconnect and preserve timeout/cancellation")
    void readMapsFailures() throws Exception {
        assertReadFailure(new IOException("broken"), ConnectionReadException.class);
        assertReadFailure(new CancellationException("cancelled"), CancellationException.class);
        // Neither a TimeoutException nor a CancellationException can originate
        // in the stream any more: a blocking read reports a lapsed deadline as
        // SocketTimeoutException, which the read loop treats as its cancellation
        // check point rather than a failure. The governor is the remaining
        // source, and readInternal must still pass its failure through
        // untranslated rather than wrapping it as a read error — the rate
        // limiter throwing is not the socket failing.
        assertGovernorFailurePassesThrough(
                new CancellationException("cancelled while metered"), CancellationException.class);

        FakeStream eof = new FakeStream();
        eof.readCounts.add(0);
        SocketConnection connection =
                new SocketConnection(ENDPOINT, noTimers(), new FakeTcpClient(eof, true), Monitors.shared());
        Throwable failure = failureOf(() -> connection.read(1, null));
        assertTrue(failure instanceof ConnectionReadException);
        assertTrue(failure.getCause() instanceof ConnectionException);
        assertEquals(ConnectionState.DISCONNECTED, connection.getState());
        connection.close();
    }

    @Test
    @DisplayName("Write chunks, governs, reports, and raises progress")
    void writesChunks() throws Exception {
        FakeStream stream = new FakeStream();
        SocketConnection connection = new SocketConnection(
                ENDPOINT, options(2, 2, 3, 100, -1, null, null), new FakeTcpClient(stream, true), Monitors.shared());
        List<int[]> reports = new ArrayList<>();
        List<Long> progress = new ArrayList<>();
        connection.addDataWrittenListener((sender, args) -> {
            assertSame(connection, sender);
            progress.add(args.getCurrentLength());
        });

        connection.write(
                5,
                new ByteArrayInputStream(new byte[] {1, 2, 3, 4, 5}),
                (requested, token) -> requested,
                (requested, granted, transferred) -> reports.add(new int[] {requested, granted, transferred}),
                null);

        assertEquals(
                List.of(List.of((byte) 1, (byte) 2), List.of((byte) 3, (byte) 4), List.of((byte) 5)), stream.writes);
        assertEquals(List.of(2L, 4L, 5L), progress);
        assertArrayEquals(new int[] {2, 2, 2}, reports.get(0));
        assertArrayEquals(new int[] {2, 2, 2}, reports.get(1));
        assertArrayEquals(new int[] {1, 1, 1}, reports.get(2));
        connection.close();
    }

    @Test
    @DisplayName("Concurrent writes serialize and expose queue depth")
    void serializesWritesAndDropsFullQueue() throws Exception {
        FakeStream stream = new FakeStream();
        CompletableFuture<Void> blocked = new CompletableFuture<>();
        stream.writeFutures.add(blocked);
        // A short queue timeout so the terminal case is reachable in a test.
        // Defect 3.4: a full queue is backpressure, not an instant kill, so the
        // drop only happens once a producer has waited this long.
        SocketConnection connection = new SocketConnection(
                ENDPOINT,
                options(8, 8, 2, 100, -1, null, null).withWriteQueueTimeout(150),
                new FakeTcpClient(stream, true),
                Monitors.shared());

        // A blocking write needs a thread apiece to contend for the queue,
        // which is what a producer is: the writers used to be futures only
        // because the write was.
        Executor writers = task -> Thread.ofVirtual().start(task);
        CompletableFuture<Void> first =
                CompletableFuture.runAsync(() -> connection.write(new byte[] {1}, null), writers);
        awaitCondition(() -> connection.getWriteQueueDepth() == 1);
        CompletableFuture<Void> second =
                CompletableFuture.runAsync(() -> connection.write(new byte[] {2}, null), writers);
        awaitCondition(() -> connection.getWriteQueueDepth() == 2);
        CompletableFuture<Void> third =
                CompletableFuture.runAsync(() -> connection.write(new byte[] {3}, null), writers);
        awaitCondition(() -> connection.getWriteQueueDepth() == 3);
        Throwable dropped = failureOf(() -> connection.write(new byte[] {4}, null));

        assertTrue(dropped instanceof ConnectionWriteDroppedException);
        assertEquals(ConnectionState.DISCONNECTED, connection.getState());
        assertTrue(futureFailure(first) instanceof ConnectionWriteException);
        assertTrue(futureFailure(second) instanceof ConnectionWriteException);
        assertTrue(futureFailure(third) instanceof ConnectionWriteException);
        connection.close();
    }

    @Test
    @DisplayName("Framed socket I/O runs only on the connection-owned writer")
    void framedWritesRunOnOwnedWriter() {
        FakeStream stream = new FakeStream();
        SocketConnection connection =
                new SocketConnection(ENDPOINT, noTimers(), new FakeTcpClient(stream, true), Monitors.shared());
        Thread caller = Thread.currentThread();

        connection.write(new byte[] {1, 2, 3}, null);

        assertEquals(1, stream.writeThreads.size());
        assertNotSame(caller, stream.writeThreads.getFirst());
        assertTrue(stream.writeThreads.getFirst().isVirtual());
        connection.close();
    }

    @Test
    @DisplayName("Cancellation removes a queued frame and preserves the connection")
    void cancellationBeforeFrameStartsDequeuesIt() throws Exception {
        FakeStream stream = new FakeStream();
        CompletableFuture<Void> blocked = new CompletableFuture<>();
        stream.writeFutures.add(blocked);
        SocketConnection connection = new SocketConnection(
                ENDPOINT, options(8, 8, 3, 100, -1, null, null), new FakeTcpClient(stream, true), Monitors.shared());
        Executor writers = task -> Thread.ofVirtual().start(task);
        CompletableFuture<Void> first =
                CompletableFuture.runAsync(() -> connection.write(new byte[] {1}, null), writers);
        awaitCondition(() -> connection.getWriteQueueDepth() == 1);

        try (CancellationController source = new CancellationController()) {
            CompletableFuture<Void> cancelled =
                    CompletableFuture.runAsync(() -> connection.write(new byte[] {2}, source.getSignal()), writers);
            awaitCondition(() -> connection.getWriteQueueDepth() == 2);
            source.cancel();

            assertTrue(futureFailure(cancelled) instanceof CancellationException);
            assertEquals(ConnectionState.CONNECTED, connection.getState());
            assertEquals(1, connection.getWriteQueueDepth());
        }

        blocked.complete(null);
        first.get(1, TimeUnit.SECONDS);
        connection.write(new byte[] {3}, null);
        assertEquals(List.of(List.of((byte) 1), List.of((byte) 3)), stream.writes);
        assertEquals(ConnectionState.CONNECTED, connection.getState());
        connection.close();
    }

    @Test
    @DisplayName("Cancellation detaches from an active frame and preserves the connection")
    void cancellationAfterFrameStartsDetaches() throws Exception {
        FakeStream stream = new FakeStream();
        CompletableFuture<Void> blocked = new CompletableFuture<>();
        stream.writeFutures.add(blocked);
        SocketConnection connection =
                new SocketConnection(ENDPOINT, noTimers(), new FakeTcpClient(stream, true), Monitors.shared());
        Executor writers = task -> Thread.ofVirtual().start(task);

        try (CancellationController source = new CancellationController()) {
            CompletableFuture<Void> cancelled =
                    CompletableFuture.runAsync(() -> connection.write(new byte[] {1}, source.getSignal()), writers);
            awaitCondition(() -> !stream.writes.isEmpty());
            source.cancel();

            assertTrue(futureFailure(cancelled) instanceof CancellationException);
            assertEquals(ConnectionState.CONNECTED, connection.getState());
        }

        blocked.complete(null);
        awaitCondition(() -> connection.getWriteQueueDepth() == 0);
        connection.write(new byte[] {2}, null);
        assertEquals(List.of(List.of((byte) 1), List.of((byte) 2)), stream.writes);
        assertEquals(ConnectionState.CONNECTED, connection.getState());
        connection.close();
    }

    @Test
    @DisplayName("Teardown promptly settles every active and queued frame")
    void teardownSettlesParkedFrameWriters() throws Exception {
        FakeStream stream = new FakeStream();
        stream.writeFutures.add(new CompletableFuture<>());
        SocketConnection connection = new SocketConnection(
                ENDPOINT, options(8, 8, 8, 100, -1, null, null), new FakeTcpClient(stream, true), Monitors.shared());
        Executor writers = task -> Thread.ofVirtual().start(task);
        List<CompletableFuture<Void>> calls = new ArrayList<>();
        for (int value = 0; value < 6; value++) {
            byte frame = (byte) value;
            calls.add(CompletableFuture.runAsync(() -> connection.write(new byte[] {frame}, null), writers));
        }
        awaitCondition(() -> connection.getWriteQueueDepth() == calls.size());

        connection.disconnect("test teardown");

        for (CompletableFuture<Void> call : calls) {
            assertTrue(futureFailure(call) instanceof ConnectionWriteException);
        }
        assertEquals(0, connection.getWriteQueueDepth());
        connection.close();
    }

    @Test
    @DisplayName("Write failures disconnect and preserve timeout/cancellation")
    void writeMapsFailures() throws Exception {
        assertWriteFailure(new IOException("broken"), ConnectionWriteException.class);
        assertWriteFailure(new CancellationException("cancelled"), CancellationException.class);
    }

    @Test
    @DisplayName("Read and write validate data and connection state")
    void validatesIoArguments() {
        FakeTcpClient connected = new FakeTcpClient(new FakeStream(), true);
        SocketConnection connection = new SocketConnection(ENDPOINT, noTimers(), connected, Monitors.shared());
        assertThrows(IllegalArgumentException.class, () -> connection.write((byte[]) null, null));
        assertThrows(IllegalArgumentException.class, () -> connection.write(new byte[0], null));
        assertThrows(
                IllegalArgumentException.class,
                () -> connection.write(0, new ByteArrayInputStream(new byte[0]), null, null, null));
        assertThrows(NullPointerException.class, () -> connection.write(1, null, null, null, null));
        assertThrows(NullPointerException.class, () -> connection.read(1, null, null, null, null));
        assertThrows(IllegalStateException.class, () -> connection.connect(null));
        connection.disconnect();
        assertThrows(IllegalStateException.class, () -> connection.read(1, null));
        assertThrows(IllegalStateException.class, () -> connection.write(new byte[] {1}, null));
        connection.close();
    }

    @Test
    @DisplayName("Asynchronous progress dispatch and handoff preserve behavior")
    void asyncProgressAndHandoff() throws Exception {
        FakeStream stream = new FakeStream();
        stream.readBytes = new byte[] {1};
        FakeTcpClient client = new FakeTcpClient(stream, true);
        // A data-read listener runs inline on the reading thread. These
        // listeners are the library's own progress counters; the one place
        // consumer code was reachable from a read loop is behind the event
        // bus's delivery thread now, so the per-connection dispatch policy this
        // used to set had nothing left to decide.
        SocketConnection connection = new SocketConnection(ENDPOINT, noTimers(), client, Monitors.shared());
        CountDownLatch event = new CountDownLatch(1);
        try {
            connection.addDataReadListener((sender, args) -> event.countDown());
            connection.read(1, null);
            assertTrue(event.await(1, TimeUnit.SECONDS));

            assertSame(client, connection.handoffTcpClient());
            connection.close();
            assertFalse(client.closed);
        } finally {
            client.close();
        }
    }

    private static void assertReadFailure(Exception cause, Class<? extends Throwable> expected) throws Exception {
        FakeStream stream = new FakeStream();
        stream.readFailure = cause;
        SocketConnection connection =
                new SocketConnection(ENDPOINT, noTimers(), new FakeTcpClient(stream, true), Monitors.shared());
        Throwable failure = failureOf(() -> connection.read(1, null));
        assertTrue(expected.isInstance(failure));
        if (failure instanceof ConnectionReadException) {
            assertSame(cause, failure.getCause());
        }
        assertEquals(ConnectionState.DISCONNECTED, connection.getState());
        connection.close();
    }

    private static void assertGovernorFailurePassesThrough(RuntimeException cause, Class<? extends Throwable> expected)
            throws Exception {
        FakeStream stream = new FakeStream();
        stream.readBytes = new byte[] {1, 2, 3};
        SocketConnection connection =
                new SocketConnection(ENDPOINT, noTimers(), new FakeTcpClient(stream, true), Monitors.shared());

        Throwable failure = failureOf(() -> connection.read(
                3,
                java.io.OutputStream.nullOutputStream(),
                (requestedBytes, token) -> {
                    throw cause;
                },
                null,
                null));

        assertTrue(expected.isInstance(failure), "expected " + expected + " but got " + failure);
        assertEquals(ConnectionState.DISCONNECTED, connection.getState());
        connection.close();
    }

    private static void assertWriteFailure(Exception cause, Class<? extends Throwable> expected) throws Exception {
        FakeStream stream = new FakeStream();
        stream.writeFailure = cause;
        SocketConnection connection =
                new SocketConnection(ENDPOINT, noTimers(), new FakeTcpClient(stream, true), Monitors.shared());
        Throwable failure = failureOf(() -> connection.write(new byte[] {1}, null));
        assertTrue(expected.isInstance(failure));
        if (failure instanceof ConnectionWriteException) {
            assertSame(cause, failure.getCause());
        }
        assertEquals(ConnectionState.DISCONNECTED, connection.getState());
        connection.close();
    }

    /**
     * The failure a blocking call raised, with {@code join()}'s wrapper off.
     *
     * <p>Blocking internals present a failure the way {@code join()} did — a
     * cancellation raw, everything else inside a {@link CompletionException} —
     * so every assertion here reads the same as it did when the operation was a
     * future.
     */
    private static Throwable failureOf(org.junit.jupiter.api.function.Executable call) {
        Throwable failure = assertThrows(Throwable.class, call);
        while (failure instanceof CompletionException && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }

    private static Throwable futureFailure(CompletableFuture<?> future) throws Exception {
        try {
            future.get(2, TimeUnit.SECONDS);
            throw new AssertionError("Future unexpectedly succeeded");
        } catch (ExecutionException exception) {
            return exception.getCause();
        } catch (CancellationException exception) {
            return exception;
        }
    }

    private static void awaitCondition(CheckedBoolean condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.get() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertTrue(condition.get());
    }

    @Test
    @DisplayName("Listeners never run while the connection monitor is held")
    void listenersRunWithoutHoldingTheConnectionLock() {
        // Defect 1.8: disconnect() used to hold synchronized(this) across both
        // changeState calls, so every listener ran under the connection's
        // monitor. Asserting holdsLock directly is deterministic, where a
        // deadlock reproduction would be timing-dependent.
        FakeTcpClient client = new FakeTcpClient(new FakeStream(), true);
        SocketConnection connection = new SocketConnection(ENDPOINT, noTimers(), client, Monitors.shared());
        List<String> heldDuring = new ArrayList<>();

        connection.addStateChangedListener((sender, args) -> {
            if (Thread.holdsLock(sender)) {
                heldDuring.add("stateChanged->" + args.getCurrentState());
            }
        });
        connection.addDisconnectedListener((sender, args) -> {
            if (Thread.holdsLock(sender)) {
                heldDuring.add("disconnected");
            }
        });

        connection.disconnect("test", null);
        connection.close();

        assertEquals(List.of(), heldDuring, "A listener ran while holding the connection monitor");
    }

    private static ConnectionOptions noTimers() {
        return options(8, 8, 3, 100, -1, null, null);
    }

    private static ConnectionOptions options(
            int readBuffer,
            int writeBuffer,
            int queue,
            int connectTimeout,
            int inactivityTimeout,
            ProxyOptions proxy,
            dev.slsk.internal.options.SocketConfigurator configurator) {
        return new ConnectionOptions(
                readBuffer, writeBuffer, queue, connectTimeout, inactivityTimeout, proxy, configurator);
    }

    @FunctionalInterface
    private interface CheckedBoolean {
        boolean get() throws Exception;
    }

    private static final class FakeTcpClient implements TcpClient {
        private final Socket socket = new Socket();
        private final FakeStream stream;
        private boolean connected;
        private boolean closed;
        private int connectCalls;
        private int proxyCalls;
        private CompletableFuture<Void> connectFuture;
        private Runnable connectAction;
        private InetAddress lastProxyAddress;
        private int lastProxyPort;
        private InetAddress lastDestination;
        private int lastDestinationPort;
        private String lastUsername;
        private String lastPassword;

        private FakeTcpClient(FakeStream stream, boolean connected) {
            this.stream = stream;
            this.connected = connected;
        }

        @Override
        public Socket getClient() {
            return socket;
        }

        @Override
        public boolean isConnected() {
            return connected && !closed;
        }

        @Override
        public InetSocketAddress getRemoteEndpoint() {
            return ENDPOINT;
        }

        @Override
        public void connect(InetAddress address, int port) {
            connectCalls++;
            if (connectAction != null) {
                connectAction.run();
            }
            if (connectFuture != null) {
                // Blocks the way a real connect does, so a test can stall one
                // and watch the caller give up on it.
                dev.slsk.internal.common.Outcomes.raise(connectFuture);
            }
        }

        @Override
        public ProxyEndpoint connectThroughProxy(
                InetAddress proxyAddress,
                int proxyPort,
                InetAddress destinationAddress,
                int destinationPort,
                String username,
                String password,
                CancellationSignal cancellationSignal) {
            proxyCalls++;
            lastProxyAddress = proxyAddress;
            lastProxyPort = proxyPort;
            lastDestination = destinationAddress;
            lastDestinationPort = destinationPort;
            lastUsername = username;
            lastPassword = password;
            if (connectAction != null) {
                connectAction.run();
            }
            if (connectFuture != null) {
                dev.slsk.internal.common.Outcomes.raise(connectFuture);
            }
            return new ProxyEndpoint("127.0.0.1", proxyPort);
        }

        @Override
        public NetworkStream getStream() {
            return stream;
        }

        @Override
        public void close() throws IOException {
            closed = true;
            connected = false;
            socket.close();
        }
    }

    private static final class FakeStream implements NetworkStream {
        private final Queue<Integer> readCounts = new ArrayDeque<>();
        private final List<Integer> readSizes = new ArrayList<>();
        private final List<List<Byte>> writes = new CopyOnWriteArrayList<>();
        private final Queue<CompletableFuture<Void>> writeFutures = new ConcurrentLinkedQueue<>();
        private final List<Thread> writeThreads = new CopyOnWriteArrayList<>();
        private volatile CompletableFuture<Void> activeWriteFuture;
        private byte[] readBytes = new byte[0];
        private int readPosition;
        private int readTimeout = -1;
        private int writeTimeout = -1;
        private Exception readFailure;
        private Exception writeFailure;

        @Override
        public int getReadTimeout() {
            return readTimeout;
        }

        @Override
        public void setReadTimeout(int timeout) {
            readTimeout = timeout;
        }

        @Override
        public int getWriteTimeout() {
            return writeTimeout;
        }

        @Override
        public void setWriteTimeout(int timeout) {
            writeTimeout = timeout;
        }

        @Override
        public int read(byte[] buffer, int offset, int size) throws IOException {
            readSizes.add(size);
            if (readFailure != null) {
                throw asIoException(readFailure);
            }
            int available = readBytes.length - readPosition;
            int count = readCounts.isEmpty() ? Math.min(size, available) : readCounts.remove();
            count = Math.min(count, Math.min(size, available));
            System.arraycopy(readBytes, readPosition, buffer, offset, count);
            readPosition += count;
            return count;
        }

        @Override
        public void write(byte[] buffer, int offset, int size) throws IOException {
            writeThreads.add(Thread.currentThread());
            if (writeFailure != null) {
                throw asIoException(writeFailure);
            }
            List<Byte> bytes = new ArrayList<>();
            for (byte value : Arrays.copyOfRange(buffer, offset, offset + size)) {
                bytes.add(value);
            }
            writes.add(bytes);
            if (!writeFutures.isEmpty()) {
                // Preserved so tests can still stall or fail a write; the
                // future is now awaited here rather than handed upward.
                activeWriteFuture = writeFutures.remove();
                try {
                    activeWriteFuture.join();
                } finally {
                    activeWriteFuture = null;
                }
            }
        }

        /**
         * Rethrows unchecked failures unchanged so tests can assert on their
         * type, and wraps checked non-IO ones, since the blocking contract only
         * permits IOException.
         */
        private static IOException asIoException(Exception failure) {
            if (failure instanceof RuntimeException unchecked) {
                throw unchecked;
            }
            return failure instanceof IOException io ? io : new IOException(failure);
        }

        @Override
        public void close() {
            IOException closed = new IOException("stream closed");
            CompletableFuture<Void> active = activeWriteFuture;
            if (active != null) {
                active.completeExceptionally(closed);
            }
            for (CompletableFuture<Void> pending : writeFutures) {
                pending.completeExceptionally(closed);
            }
        }
    }
}
