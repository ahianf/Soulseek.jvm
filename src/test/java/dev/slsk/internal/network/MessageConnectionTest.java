// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.exceptions.ConnectionWriteException;
import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.common.Monitors;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.network.tcp.ConnectionKey;
import dev.slsk.internal.network.tcp.NetworkStream;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MessageConnectionTest {
    private static final InetSocketAddress ENDPOINT = new InetSocketAddress(InetAddress.getLoopbackAddress(), 2234);
    private static final ConnectionOptions OPTIONS = ConnectionOptions.builder()
            .readBufferSize(8)
            .writeBufferSize(8)
            .writeQueueSize(3)
            .connectTimeout(Duration.ofMillis(100))
            .inactivityTimeout(null)
            .build();

    @Test
    @DisplayName("Message connection distinguishes server and peer identity")
    void constructsServerAndPeer() {
        try (DefaultMessageConnection server =
                        new DefaultMessageConnection(ENDPOINT, OPTIONS, 4, null, Monitors.shared());
                DefaultMessageConnection peer =
                        new DefaultMessageConnection("alice", ENDPOINT, OPTIONS, 1, null, Monitors.shared())) {
            assertTrue(server.isServerConnection());
            assertEquals("", server.getUsername());
            assertEquals(4, server.getCodeLength());
            assertEquals(new ConnectionKey("", ENDPOINT), server.getKey());
            assertFalse(peer.isServerConnection());
            assertEquals("alice", peer.getUsername());
            assertEquals(1, peer.getCodeLength());
            assertEquals(new ConnectionKey("alice", ENDPOINT), peer.getKey());
            assertFalse(server.isReadingContinuously());
            assertFalse(peer.isReadingContinuously());
        }
    }

    @Test
    @DisplayName("Peer username rejects null, empty, and whitespace")
    void validatesUsername() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new DefaultMessageConnection(null, ENDPOINT, OPTIONS, 4, null, Monitors.shared()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new DefaultMessageConnection("", ENDPOINT, OPTIONS, 4, null, Monitors.shared()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new DefaultMessageConnection(" \t\u00a0", ENDPOINT, OPTIONS, 4, null, Monitors.shared()));
    }

    @Test
    @DisplayName("Connected event starts the continuous read loop")
    void connectStartsReading() throws Exception {
        FakeStream stream = new FakeStream();
        stream.blockReads = true;
        FakeTcpClient client = new FakeTcpClient(stream, false);
        client.connectAction = () -> client.connected = true;
        DefaultMessageConnection connection =
                new DefaultMessageConnection(ENDPOINT, OPTIONS, 4, client, Monitors.shared());

        connection.connect(null);
        awaitCondition(connection::isReadingContinuously);

        assertTrue(connection.isReadingContinuously());
        connection.close();
        awaitCondition(() -> !connection.isReadingContinuously());
    }

    @Test
    @DisplayName("Adopted connection waits for explicit read-loop start")
    void explicitStartReadsAdoptedConnection() throws Exception {
        FakeStream stream = new FakeStream();
        stream.blockReads = true;
        FakeTcpClient client = new FakeTcpClient(stream, true);
        DefaultMessageConnection connection =
                new DefaultMessageConnection("alice", ENDPOINT, OPTIONS, 4, client, Monitors.shared());

        assertFalse(connection.isReadingContinuously());
        connection.startReadingContinuously();
        awaitCondition(connection::isReadingContinuously);
        // A second entry returns at once rather than starting a second loop.
        connection.readContinuously();

        connection.close();
        awaitCondition(() -> !connection.isReadingContinuously());
    }

    @Test
    @DisplayName("Continuous read raises header, progress, and full-message events")
    void readsFramedMessage() throws Exception {
        byte[] code = new byte[] {9, 8, 7, 6};
        byte[] payload = new byte[] {1, 2, 3};
        byte[] frame = frame(code, payload);
        FakeStream stream = new FakeStream(frame);
        stream.maxRead = 1;
        FakeTcpClient client = new FakeTcpClient(stream, true);
        DefaultMessageConnection connection =
                new DefaultMessageConnection("alice", ENDPOINT, OPTIONS, 4, client, Monitors.shared());
        AtomicReference<MessageReceivedEvent> received = new AtomicReference<>();
        AtomicReference<MessageEvent> read = new AtomicReference<>();
        List<MessageDataEvent> progress = new ArrayList<>();
        CountDownLatch complete = new CountDownLatch(1);
        connection.<MessageReceivedEvent>subscribe(MessageConnection.MessageKind.RECEIVED, args -> {
            assertSame(connection, args.connection());
            received.set(args);
        });
        connection.<MessageDataEvent>subscribe(MessageConnection.MessageKind.DATA_READ, args -> {
            assertSame(connection, args.connection());
            progress.add(args);
        });
        connection.<MessageEvent>subscribe(MessageConnection.MessageKind.READ, args -> {
            assertSame(connection, args.connection());
            read.set(args);
            complete.countDown();
        });

        connection.startReadingContinuously();
        assertTrue(complete.await(1, TimeUnit.SECONDS));

        assertEquals(7, received.get().length());
        assertSame(received.get().code(), progress.get(0).code());
        assertArrayEquals(code, received.get().code());
        assertArrayEquals(frame, read.get().message());
        assertEquals(
                List.of(0L, 1L, 2L, 3L),
                progress.stream().map(MessageDataEvent::currentLength).toList());
        assertEquals(3, progress.get(0).totalLength());
        assertEquals(100.0, progress.get(3).percentComplete());
        connection.close();
    }

    @Test
    @DisplayName("Read-loop failure disconnects and reports the cause")
    void readLoopFailureDisconnectsWithCause() throws Exception {
        // A throwing message-received listener is the cleanest way to inject a
        // failure the read path does not already handle: that event is raised
        // synchronously on the loop thread, so it escapes the loop. Before
        // defect 1.7 the failure was discarded and the connection sat there
        // looking healthy with a dead read loop.
        byte[] frame = frame(new byte[] {1, 2, 3, 4}, new byte[] {5, 6});
        FakeStream stream = new FakeStream(frame);
        FakeTcpClient client = new FakeTcpClient(stream, true);
        DefaultMessageConnection connection =
                new DefaultMessageConnection("alice", ENDPOINT, OPTIONS, 4, client, Monitors.shared());

        IllegalStateException injected = new IllegalStateException("listener exploded");
        connection.<MessageReceivedEvent>subscribe(MessageConnection.MessageKind.RECEIVED, args -> {
            throw injected;
        });

        connection.startReadingContinuously();

        IllegalStateException thrown =
                assertThrows(IllegalStateException.class, () -> connection.awaitDisconnect(null));
        assertSame(injected, thrown);
        connection.close();
    }

    @Test
    @DisplayName("Continuous read supports one-byte distributed codes")
    void readsOneByteCode() throws Exception {
        byte[] frame = frame(new byte[] {(byte) 0x93}, new byte[] {4, 5});
        FakeStream stream = new FakeStream(frame);
        DefaultMessageConnection connection = new DefaultMessageConnection(
                "alice", ENDPOINT, OPTIONS, 1, new FakeTcpClient(stream, true), Monitors.shared());
        AtomicReference<byte[]> read = new AtomicReference<>();
        CountDownLatch complete = new CountDownLatch(1);
        connection.<MessageEvent>subscribe(MessageConnection.MessageKind.READ, args -> {
            read.set(args.message());
            complete.countDown();
        });

        connection.startReadingContinuously();

        assertTrue(complete.await(1, TimeUnit.SECONDS));
        assertArrayEquals(frame, read.get());
        connection.close();
    }

    @Test
    @DisplayName("Message write serializes once and raises after completion")
    void writesMessage() throws Exception {
        FakeStream stream = new FakeStream();
        FakeTcpClient client = new FakeTcpClient(stream, true);
        DefaultMessageConnection connection =
                new DefaultMessageConnection("alice", ENDPOINT, OPTIONS, 4, client, Monitors.shared());
        byte[] bytes = new byte[] {4, 0, 0, 0, 1, 2, 3, 4};
        AtomicReference<MessageEvent> written = new AtomicReference<>();
        AtomicReference<CancellationSignal> tokenSeen = new AtomicReference<>();
        stream.tokenSeen = tokenSeen;
        CancellationSignal token = CancellationSignal.none();
        connection.<MessageEvent>subscribe(MessageConnection.MessageKind.WRITTEN, args -> {
            assertSame(connection, args.connection());
            written.set(args);
        });

        connection.write(() -> bytes, token);

        assertArrayEquals(bytes, stream.writtenBytes());
        assertSame(bytes, written.get().message());
        assertSame(token, tokenSeen.get());
        connection.close();
    }

    @Test
    @DisplayName("Message write validates serialization and state")
    void validatesWrite() {
        DefaultMessageConnection disconnected =
                new DefaultMessageConnection("alice", ENDPOINT, null, 4, null, Monitors.shared());
        assertThrows(NullPointerException.class, () -> disconnected.write((OutgoingMessage) null));
        RuntimeException cause = new RuntimeException("broken");
        MessageException serialization = assertThrows(
                MessageException.class,
                () -> disconnected.write(() -> {
                    throw cause;
                }));
        assertSame(cause, serialization.getCause());
        assertThrows(IllegalStateException.class, () -> disconnected.write(() -> new byte[] {1}));
        disconnected.close();
    }

    @Test
    @DisplayName("Message write maps stream failure through SocketConnection")
    void writeFailureMaps() throws Exception {
        FakeStream stream = new FakeStream();
        IOException cause = new IOException("broken");
        stream.writeFailure = cause;
        DefaultMessageConnection connection =
                new DefaultMessageConnection(ENDPOINT, OPTIONS, 4, new FakeTcpClient(stream, true), Monitors.shared());

        ConnectionWriteException failure =
                assertThrows(ConnectionWriteException.class, () -> connection.write(() -> new byte[] {1}));

        assertSame(cause, failure.getCause());
        connection.close();
    }

    @Test
    @DisplayName("Message event data preserves source array identity and arithmetic")
    void eventDataPreserveData() {
        byte[] code = new byte[] {1};
        byte[] message = new byte[] {2};
        MessageDataEvent progress = new MessageDataEvent(code, 0, 0);
        MessageReceivedEvent received = new MessageReceivedEvent(5, code);
        MessageEvent complete = new MessageEvent(message);

        assertSame(code, progress.code());
        assertTrue(Double.isNaN(progress.percentComplete()));
        assertSame(code, received.code());
        assertEquals(5, received.length());
        assertSame(message, complete.message());
    }

    private static byte[] frame(byte[] code, byte[] payload) {
        byte[] result = new byte[4 + code.length + payload.length];
        ByteBuffer.wrap(result)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(code.length + payload.length)
                .put(code)
                .put(payload);
        return result;
    }

    private static void awaitCondition(CheckedBoolean condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.get() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertTrue(condition.get());
    }

    @FunctionalInterface
    private interface CheckedBoolean {
        boolean get() throws Exception;
    }

    private static final class FakeTcpClient implements TcpClient {
        private final Socket socket = new Socket();
        private final FakeStream stream;
        private boolean connected;
        private Runnable connectAction;

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
            if (connectAction != null) {
                connectAction.run();
            }
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
        private final List<Byte> written = new ArrayList<>();
        private byte[] input;
        private int position;
        private int maxRead = Integer.MAX_VALUE;
        private boolean blockReads;
        private CompletableFuture<Integer> blockedRead;
        private Exception writeFailure;
        private AtomicReference<CancellationSignal> tokenSeen;

        private FakeStream() {
            this(new byte[0]);
        }

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
        public int read(byte[] buffer, int offset, int size) throws IOException {
            CompletableFuture<Integer> gate;
            synchronized (this) {
                if (!blockReads) {
                    int count = Math.min(Math.min(size, maxRead), input.length - position);
                    if (count <= 0) {
                        return 0;
                    }
                    System.arraycopy(input, position, buffer, offset, count);
                    position += count;
                    return count;
                }
                blockedRead = new CompletableFuture<>();
                gate = blockedRead;
            }
            // Blocked outside the monitor so close() can release it. A blocking
            // read models a stalled peer directly now, where the async fake
            // returned a never-completing future.
            try {
                return gate.join();
            } catch (RuntimeException closed) {
                throw new IOException("stream closed", closed);
            }
        }

        @Override
        public synchronized void write(byte[] buffer, int offset, int size) throws IOException {
            if (tokenSeen != null) {
                tokenSeen.set(CancellationSignal.none());
            }
            if (writeFailure != null) {
                throw writeFailure instanceof IOException io ? io : new IOException(writeFailure);
            }
            for (byte value : Arrays.copyOfRange(buffer, offset, offset + size)) {
                written.add(value);
            }
        }

        private byte[] writtenBytes() {
            byte[] result = new byte[written.size()];
            for (int index = 0; index < result.length; index++) {
                result[index] = written.get(index);
            }
            return result;
        }

        @Override
        public synchronized void close() {
            if (blockedRead != null) {
                blockedRead.completeExceptionally(new CancellationException("closed"));
            }
        }
    }
}
