// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationToken;
import dev.slsk.CancellationTokenSource;
import dev.slsk.common.EventDispatch;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.ConnectionReadException;
import dev.slsk.exceptions.ConnectionWriteDroppedException;
import dev.slsk.exceptions.ConnectionWriteException;
import dev.slsk.options.ConnectionOptions;
import dev.slsk.options.ProxyOptions;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
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
        try (SocketConnection connection = new SocketConnection(ENDPOINT)) {
            assertSame(ENDPOINT, connection.getIpEndPoint());
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

        try (SocketConnection connection = new SocketConnection(ENDPOINT, options, client)) {
            assertSame(client.socket, configured.get());
            assertEquals(1_000, client.socket.getSoTimeout());
            assertEquals(1_000, stream.readTimeout);
            assertEquals(1_000, stream.writeTimeout);
            assertEquals(ConnectionState.CONNECTED, connection.getState());
            assertSame(options, connection.getOptions());
        }
    }

    @Test
    @DisplayName("Disconnect raises both state changes and completion")
    void disconnectRaisesEvents() throws Exception {
        FakeTcpClient client = new FakeTcpClient(new FakeStream(), true);
        SocketConnection connection = new SocketConnection(ENDPOINT, noTimers(), client);
        List<ConnectionStateChangedEventArgs> states = new ArrayList<>();
        List<ConnectionDisconnectedEventArgs> disconnected = new ArrayList<>();
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
        assertEquals("done", connection.waitForDisconnect(null).get(1, TimeUnit.SECONDS));
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
        SocketConnection connection = new SocketConnection(ENDPOINT, noTimers(), client);
        List<ConnectionState> states = new ArrayList<>();
        AtomicInteger connected = new AtomicInteger();
        connection.addStateChangedListener((sender, args) -> states.add(args.getCurrentState()));
        connection.addConnectedListener((sender, args) -> connected.incrementAndGet());

        connection.connectAsync(null).get(1, TimeUnit.SECONDS);

        assertEquals(1, client.connectCalls);
        assertEquals(0, client.proxyCalls);
        assertEquals(List.of(ConnectionState.CONNECTING, ConnectionState.CONNECTED), states);
        assertEquals(1, connected.get());
        assertEquals(ConnectionState.CONNECTED, connection.getState());
        assertEquals(-1, stream.readTimeout);
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
        SocketConnection connection = new SocketConnection(ENDPOINT, options, client);

        connection.connectAsync(CancellationToken.none()).get(1, TimeUnit.SECONDS);

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
        SocketConnection connection = new SocketConnection(ENDPOINT, options(8, 8, 3, 10, -1, null, null), client);

        Throwable failure = futureFailure(connection.connectAsync(null));

        assertTrue(failure instanceof TimeoutException);
        assertEquals(ConnectionState.DISCONNECTED, connection.getState());
        connection.close();
    }

    @Test
    @DisplayName("Connect cancellation disconnects without remapping")
    void connectCancels() throws Exception {
        FakeTcpClient client = new FakeTcpClient(new FakeStream(), false);
        client.connectFuture = new CompletableFuture<>();
        SocketConnection connection = new SocketConnection(ENDPOINT, options(8, 8, 3, 5_000, -1, null, null), client);

        try (CancellationTokenSource source = new CancellationTokenSource()) {
            source.cancel();
            assertThrows(
                    CancellationException.class,
                    () -> connection.connectAsync(source.getToken()).get(1, TimeUnit.SECONDS));
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
        SocketConnection connection = new SocketConnection(ENDPOINT, noTimers(), client);

        Throwable failure = futureFailure(connection.connectAsync(null));

        assertTrue(failure instanceof ConnectionException);
        assertSame(cause, failure.getCause());
        assertEquals(ConnectionState.DISCONNECTED, connection.getState());
        connection.close();
    }

    @Test
    @DisplayName("Inactivity timer and watchdog disconnect independently")
    void timersDisconnect() throws Exception {
        FakeTcpClient inactiveClient = new FakeTcpClient(new FakeStream(), true);
        SocketConnection inactive =
                new SocketConnection(ENDPOINT, options(8, 8, 3, 100, 20, null, null), inactiveClient);
        assertTrue(futureFailure(inactive.waitForDisconnect(null)) instanceof TimeoutException);
        inactive.close();

        FakeTcpClient watchedClient = new FakeTcpClient(new FakeStream(), true);
        SocketConnection watched = new SocketConnection(ENDPOINT, noTimers(), watchedClient);
        watchedClient.connected = false;
        Throwable watchdogFailure = futureFailure(watched.waitForDisconnect(null));
        assertTrue(watchdogFailure instanceof ConnectionException);
        assertEquals("The connection was closed unexpectedly", watchdogFailure.getMessage());
        watched.close();
    }

    @Test
    @DisplayName("Wait cancellation disconnects the connection")
    void waitCancellationDisconnects() {
        FakeTcpClient client = new FakeTcpClient(new FakeStream(), true);
        SocketConnection connection = new SocketConnection(ENDPOINT, noTimers(), client);
        try (CancellationTokenSource source = new CancellationTokenSource()) {
            CompletableFuture<String> wait = connection.waitForDisconnect(source.getToken());
            source.cancel();
            assertThrows(CancellationException.class, wait::join);
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
        SocketConnection connection = new SocketConnection(ENDPOINT, options(2, 2, 3, 100, -1, null, null), client);
        List<int[]> reports = new ArrayList<>();
        List<Long> progress = new ArrayList<>();
        connection.addDataReadListener((sender, args) -> {
            assertSame(connection, sender);
            progress.add(args.getCurrentLength());
        });
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        connection
                .readAsync(
                        5,
                        output,
                        (requested, token) -> CompletableFuture.completedFuture(requested),
                        (requested, granted, transferred) -> reports.add(new int[] {requested, granted, transferred}),
                        null)
                .get(1, TimeUnit.SECONDS);

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
        SocketConnection connection =
                new SocketConnection(ENDPOINT, options(2, 2, 3, 100, -1, null, null), new FakeTcpClient(stream, true));

        assertArrayEquals(new byte[0], connection.readAsync(0, null).get(1, TimeUnit.SECONDS));
        assertArrayEquals(new byte[] {9, 8, 7}, connection.readAsync(3L, null).get(1, TimeUnit.SECONDS));
        assertThrows(IllegalArgumentException.class, () -> connection.readAsync(-1, null));
        connection.close();
    }

    @Test
    @DisplayName("Read failures disconnect and preserve timeout/cancellation")
    void readMapsFailures() throws Exception {
        assertReadFailure(new IOException("broken"), ConnectionReadException.class);
        assertReadFailure(new TimeoutException("late"), TimeoutException.class);
        assertReadFailure(new CancellationException("cancelled"), CancellationException.class);

        FakeStream eof = new FakeStream();
        eof.readCounts.add(0);
        SocketConnection connection = new SocketConnection(ENDPOINT, noTimers(), new FakeTcpClient(eof, true));
        Throwable failure = futureFailure(connection.readAsync(1, null));
        assertTrue(failure instanceof ConnectionReadException);
        assertTrue(failure.getCause() instanceof ConnectionException);
        assertEquals(ConnectionState.DISCONNECTED, connection.getState());
        connection.close();
    }

    @Test
    @DisplayName("Write chunks, governs, reports, and raises progress")
    void writesChunks() throws Exception {
        FakeStream stream = new FakeStream();
        SocketConnection connection =
                new SocketConnection(ENDPOINT, options(2, 2, 3, 100, -1, null, null), new FakeTcpClient(stream, true));
        List<int[]> reports = new ArrayList<>();
        List<Long> progress = new ArrayList<>();
        connection.addDataWrittenListener((sender, args) -> {
            assertSame(connection, sender);
            progress.add(args.getCurrentLength());
        });

        connection
                .writeAsync(
                        5,
                        new ByteArrayInputStream(new byte[] {1, 2, 3, 4, 5}),
                        (requested, token) -> CompletableFuture.completedFuture(requested),
                        (requested, granted, transferred) -> reports.add(new int[] {requested, granted, transferred}),
                        null)
                .get(1, TimeUnit.SECONDS);

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
        SocketConnection connection =
                new SocketConnection(ENDPOINT, options(8, 8, 2, 100, -1, null, null), new FakeTcpClient(stream, true));

        CompletableFuture<Void> first = connection.writeAsync(new byte[] {1}, null);
        awaitCondition(() -> connection.getWriteQueueDepth() == 1);
        CompletableFuture<Void> second = connection.writeAsync(new byte[] {2}, null);
        awaitCondition(() -> connection.getWriteQueueDepth() == 2);
        Throwable dropped = futureFailure(connection.writeAsync(new byte[] {3}, null));

        assertTrue(dropped instanceof ConnectionWriteDroppedException);
        assertEquals(ConnectionState.DISCONNECTED, connection.getState());
        blocked.complete(null);
        first.get(1, TimeUnit.SECONDS);
        assertTrue(futureFailure(second) instanceof ConnectionWriteException);
        connection.close();
    }

    @Test
    @DisplayName("Write failures disconnect and preserve timeout/cancellation")
    void writeMapsFailures() throws Exception {
        assertWriteFailure(new IOException("broken"), ConnectionWriteException.class);
        assertWriteFailure(new TimeoutException("late"), TimeoutException.class);
        assertWriteFailure(new CancellationException("cancelled"), CancellationException.class);
    }

    @Test
    @DisplayName("Read and write validate data and connection state")
    void validatesIoArguments() {
        FakeTcpClient connected = new FakeTcpClient(new FakeStream(), true);
        SocketConnection connection = new SocketConnection(ENDPOINT, noTimers(), connected);
        assertThrows(IllegalArgumentException.class, () -> connection.writeAsync(null, null));
        assertThrows(IllegalArgumentException.class, () -> connection.writeAsync(new byte[0], null));
        assertThrows(
                IllegalArgumentException.class,
                () -> connection.writeAsync(0, new ByteArrayInputStream(new byte[0]), null, null, null));
        assertThrows(NullPointerException.class, () -> connection.writeAsync(1, null, null, null, null));
        assertThrows(NullPointerException.class, () -> connection.readAsync(1, null, null, null, null));
        assertThrows(IllegalStateException.class, () -> connection.connectAsync(null));
        connection.disconnect();
        assertThrows(IllegalStateException.class, () -> connection.readAsync(1, null));
        assertThrows(IllegalStateException.class, () -> connection.writeAsync(new byte[] {1}, null));
        connection.close();
    }

    @Test
    @DisplayName("Asynchronous progress dispatch and handoff preserve behavior")
    void asyncProgressAndHandoff() throws Exception {
        FakeStream stream = new FakeStream();
        stream.readBytes = new byte[] {1};
        FakeTcpClient client = new FakeTcpClient(stream, true);
        SocketConnection connection = new SocketConnection(ENDPOINT, noTimers(), client);
        CountDownLatch event = new CountDownLatch(1);
        EventDispatch.setAsynchronous(true);
        try {
            connection.addDataReadListener((sender, args) -> event.countDown());
            connection.readAsync(1, null).get(1, TimeUnit.SECONDS);
            assertTrue(event.await(1, TimeUnit.SECONDS));

            assertSame(client, connection.handoffTcpClient());
            connection.close();
            assertFalse(client.closed);
        } finally {
            EventDispatch.setAsynchronous(false);
            client.close();
        }
    }

    private static void assertReadFailure(Exception cause, Class<? extends Throwable> expected) throws Exception {
        FakeStream stream = new FakeStream();
        stream.readFailure = cause;
        SocketConnection connection = new SocketConnection(ENDPOINT, noTimers(), new FakeTcpClient(stream, true));
        Throwable failure;
        if (cause instanceof CancellationException) {
            failure = assertThrows(
                    CancellationException.class,
                    () -> connection.readAsync(1, null).get());
        } else {
            failure = futureFailure(connection.readAsync(1, null));
        }
        assertTrue(expected.isInstance(failure));
        if (failure instanceof ConnectionReadException) {
            assertSame(cause, failure.getCause());
        }
        assertEquals(ConnectionState.DISCONNECTED, connection.getState());
        connection.close();
    }

    private static void assertWriteFailure(Exception cause, Class<? extends Throwable> expected) throws Exception {
        FakeStream stream = new FakeStream();
        stream.writeFailure = cause;
        SocketConnection connection = new SocketConnection(ENDPOINT, noTimers(), new FakeTcpClient(stream, true));
        Throwable failure;
        if (cause instanceof CancellationException) {
            failure = assertThrows(
                    CancellationException.class,
                    () -> connection.writeAsync(new byte[] {1}, null).get());
        } else {
            failure = futureFailure(connection.writeAsync(new byte[] {1}, null));
        }
        assertTrue(expected.isInstance(failure));
        if (failure instanceof ConnectionWriteException) {
            assertSame(cause, failure.getCause());
        }
        assertEquals(ConnectionState.DISCONNECTED, connection.getState());
        connection.close();
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
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (!condition.get() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertTrue(condition.get());
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
            dev.slsk.options.SocketConfigurator configurator) {
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
        public InetSocketAddress getRemoteEndPoint() {
            return ENDPOINT;
        }

        @Override
        public CompletableFuture<Void> connectAsync(InetAddress address, int port) {
            connectCalls++;
            if (connectAction != null) {
                connectAction.run();
            }
            return connectFuture == null ? CompletableFuture.completedFuture(null) : connectFuture;
        }

        @Override
        public CompletableFuture<ProxyEndpoint> connectThroughProxyAsync(
                InetAddress proxyAddress,
                int proxyPort,
                InetAddress destinationAddress,
                int destinationPort,
                String username,
                String password,
                CancellationToken cancellationToken) {
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
                return connectFuture.thenApply(ignored -> new ProxyEndpoint("127.0.0.1", proxyPort));
            }
            return CompletableFuture.completedFuture(new ProxyEndpoint("127.0.0.1", proxyPort));
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
        private final List<List<Byte>> writes = new ArrayList<>();
        private final Queue<CompletableFuture<Void>> writeFutures = new ArrayDeque<>();
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
        public CompletableFuture<Integer> readAsync(
                byte[] buffer, int offset, int size, CancellationToken cancellationToken) {
            readSizes.add(size);
            if (readFailure != null) {
                return CompletableFuture.failedFuture(readFailure);
            }
            int available = readBytes.length - readPosition;
            int count = readCounts.isEmpty() ? Math.min(size, available) : readCounts.remove();
            count = Math.min(count, Math.min(size, available));
            System.arraycopy(readBytes, readPosition, buffer, offset, count);
            readPosition += count;
            return CompletableFuture.completedFuture(count);
        }

        @Override
        public CompletableFuture<Void> writeAsync(
                byte[] buffer, int offset, int size, CancellationToken cancellationToken) {
            if (writeFailure != null) {
                return CompletableFuture.failedFuture(writeFailure);
            }
            List<Byte> bytes = new ArrayList<>();
            for (byte value : Arrays.copyOfRange(buffer, offset, offset + size)) {
                bytes.add(value);
            }
            writes.add(bytes);
            return writeFutures.isEmpty() ? CompletableFuture.completedFuture(null) : writeFutures.remove();
        }

        @Override
        public void close() {}
    }
}
