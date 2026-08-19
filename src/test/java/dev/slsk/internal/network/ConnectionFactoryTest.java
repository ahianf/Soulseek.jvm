// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.common.Monitors;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.network.tcp.Connection;
import dev.slsk.internal.network.tcp.NetworkStream;
import dev.slsk.internal.network.tcp.SocketConnection;
import dev.slsk.internal.network.tcp.TcpClient;
import dev.slsk.internal.options.ConnectionOptions;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConnectionFactoryTest {
    private static final InetSocketAddress ENDPOINT = new InetSocketAddress(InetAddress.getLoopbackAddress(), 2234);

    @Test
    @DisplayName("Factory creates transfer connection with supplied options")
    void createsTransferConnection() {
        ConnectionOptions options = options(1, 2, 3, 4, null);
        try (Connection connection =
                new DefaultConnectionFactory(Monitors.shared()).getTransferConnection(ENDPOINT, options, null)) {
            assertTrue(connection instanceof SocketConnection);
            assertSame(ENDPOINT, connection.getIpEndpoint());
            assertSame(options, connection.getOptions());
        }

        try (Connection defaults = new DefaultConnectionFactory(Monitors.shared()).getTransferConnection(ENDPOINT)) {
            assertNotNull(defaults.getOptions());
        }
    }

    @Test
    @DisplayName("Factory creates peer and distributed message variants")
    void createsPeerVariants() {
        ConnectionOptions options = options(1, 2, 3, 4, null);
        DefaultConnectionFactory factory = new DefaultConnectionFactory(Monitors.shared());
        try (MessageConnection peer = factory.getMessageConnection("alice", ENDPOINT, options, null);
                MessageConnection distributed = factory.getDistributedConnection("alice", ENDPOINT, options, null)) {
            assertSame(options, peer.getOptions());
            assertEquals(4, peer.getCodeLength());
            assertSame(options, distributed.getOptions());
            assertEquals(1, distributed.getCodeLength());
        }
        try (MessageConnection peer = factory.getMessageConnection("alice", ENDPOINT);
                MessageConnection distributed = factory.getDistributedConnection("alice", ENDPOINT)) {
            assertNotNull(peer.getOptions());
            assertNotNull(distributed.getOptions());
        }
    }

    @Test
    @DisplayName("Server factory disables inactivity and binds all handlers")
    void createsServerConnectionAndBindsHandlers() throws Exception {
        byte[] frame = ByteBuffer.allocate(8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(4)
                .putInt(42)
                .array();
        FakeStream stream = new FakeStream(frame);
        FakeTcpClient client = new FakeTcpClient(stream);
        AtomicInteger connected = new AtomicInteger();
        AtomicInteger disconnected = new AtomicInteger();
        AtomicInteger read = new AtomicInteger();
        AtomicInteger written = new AtomicInteger();
        CountDownLatch readEvent = new CountDownLatch(1);
        ConnectionOptions options = options(8, 8, 3, 100, Duration.ofMillis(50));

        MessageConnection connection = new DefaultConnectionFactory(Monitors.shared())
                .getServerConnection(
                        ENDPOINT,
                        (sender, args) -> connected.incrementAndGet(),
                        (sender, args) -> disconnected.incrementAndGet(),
                        (sender, args) -> {
                            read.incrementAndGet();
                            readEvent.countDown();
                        },
                        (sender, args) -> written.incrementAndGet(),
                        options,
                        client);

        client.connectAction = () -> client.connected = true;
        connection.connect(null);
        assertTrue(readEvent.await(1, TimeUnit.SECONDS));
        assertEquals(1, connected.get());
        assertEquals(1, read.get());
        assertNull(connection.getOptions().inactivityTimeout());

        client.connected = true;
        connection.write(() -> new byte[] {1});
        assertEquals(1, written.get());

        connection.disconnect("done");
        assertTrue(disconnected.get() >= 1);
        connection.close();
    }

    @Test
    @DisplayName("Server factory accepts null handlers and default options")
    void serverDefaults() {
        try (MessageConnection connection =
                new DefaultConnectionFactory(Monitors.shared()).getServerConnection(ENDPOINT, null, null, null, null)) {
            assertNotNull(connection.getOptions());
            assertNull(connection.getOptions().inactivityTimeout());
        }
    }

    private static ConnectionOptions options(
            int readBufferSize,
            int writeBufferSize,
            int writeQueueSize,
            long connectTimeoutMillis,
            Duration inactivityTimeout) {
        return ConnectionOptions.builder()
                .readBufferSize(readBufferSize)
                .writeBufferSize(writeBufferSize)
                .writeQueueSize(writeQueueSize)
                .connectTimeout(Duration.ofMillis(connectTimeoutMillis))
                .inactivityTimeout(inactivityTimeout)
                .build();
    }

    private static final class FakeTcpClient implements TcpClient {
        private final Socket socket = new Socket();
        private final FakeStream stream;
        private boolean connected;
        private Runnable connectAction;

        private FakeTcpClient(FakeStream stream) {
            this.stream = stream;
        }

        @Override
        public Socket getClient() {
            return socket;
        }

        @Override
        public boolean isConnected() {
            return connected && !socket.isClosed();
        }

        @Override
        public InetSocketAddress getRemoteEndpoint() {
            return ENDPOINT;
        }

        @Override
        public void connect(InetAddress address, int port) {
            if (connectAction != null) {
                connectAction.run();
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
            return new ProxyEndpoint("127.0.0.1", proxyPort);
        }

        @Override
        public NetworkStream getStream() {
            return stream;
        }

        @Override
        public void close() throws IOException {
            connected = false;
            stream.close();
            socket.close();
        }
    }

    private static final class FakeStream implements NetworkStream {
        private final byte[] input;
        private final List<Byte> written = new ArrayList<>();
        private int position;
        private CompletableFuture<Integer> pendingRead;

        private FakeStream(byte[] input) {
            this.input = input;
        }

        @Override
        public int getReadTimeout() {
            return -1;
        }

        @Override
        public void setReadTimeout(int timeout) {}

        @Override
        public int getWriteTimeout() {
            return -1;
        }

        @Override
        public void setWriteTimeout(int timeout) {}

        @Override
        public int read(byte[] buffer, int offset, int size) throws java.io.IOException {
            CompletableFuture<Integer> gate;
            synchronized (this) {
                int count = Math.min(size, input.length - position);
                if (count != 0) {
                    System.arraycopy(input, position, buffer, offset, count);
                    position += count;
                    return count;
                }
                pendingRead = new CompletableFuture<>();
                gate = pendingRead;
            }
            // Blocked outside the monitor so close() can release it.
            try {
                return gate.join();
            } catch (RuntimeException closed) {
                throw new java.io.IOException("stream closed", closed);
            }
        }

        @Override
        public synchronized void write(byte[] buffer, int offset, int size) {
            for (byte value : Arrays.copyOfRange(buffer, offset, offset + size)) {
                written.add(value);
            }
        }

        @Override
        public synchronized void close() {
            if (pendingRead != null) {
                pendingRead.completeExceptionally(new java.util.concurrent.CancellationException("closed"));
            }
        }
    }
}
