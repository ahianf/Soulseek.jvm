// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationToken;
import dev.slsk.common.Constants;
import dev.slsk.common.IWaiter;
import dev.slsk.common.WaitKey;
import dev.slsk.diagnostics.DiagnosticEventListener;
import dev.slsk.diagnostics.IDiagnosticFactory;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.messaging.handlers.IPeerMessageHandler;
import dev.slsk.messaging.messages.ConnectToPeerRequest;
import dev.slsk.messaging.messages.ConnectToPeerResponse;
import dev.slsk.messaging.messages.IOutgoingMessage;
import dev.slsk.messaging.messages.PeerInit;
import dev.slsk.messaging.messages.PierceFirewall;
import dev.slsk.network.tcp.ConnectionDisconnectedEventArgs;
import dev.slsk.network.tcp.ConnectionEventListener;
import dev.slsk.network.tcp.ConnectionKey;
import dev.slsk.network.tcp.ConnectionState;
import dev.slsk.network.tcp.ConnectionTypes;
import dev.slsk.network.tcp.IConnection;
import dev.slsk.network.tcp.ITcpClient;
import dev.slsk.options.ConnectionOptions;
import dev.slsk.options.SoulseekClientOptions;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PeerConnectionManagerTest {
    private static final InetSocketAddress DIRECT_ENDPOINT = endpoint(41001);
    private static final InetSocketAddress INDIRECT_ENDPOINT = endpoint(41002);
    private static final String LOCAL_USER = "local";
    private static final String USERNAME = "peer";
    private static final int TOKEN = 0x12345678;

    @Test
    void constructionDiagnosticsAndClosePreserveSourceLifecycle() {
        assertThrows(NullPointerException.class, () -> new PeerConnectionManager(null));

        Fixture fixture = new Fixture();
        PeerConnectionManager manager = fixture.manager();
        AtomicInteger events = new AtomicInteger();
        DiagnosticEventListener listener = (sender, args) -> events.incrementAndGet();
        manager.addDiagnosticGeneratedListener(listener);

        // The supplied diagnostic remains usable and close is idempotent.
        fixture.diagnostic.info("test");
        assertEquals(0, events.get());
        assertTrue(manager.getMessageConnections().isEmpty());
        assertTrue(manager.getPendingSolicitations().isEmpty());
        assertDoesNotThrow(manager::close);
        assertDoesNotThrow(manager::close);

        PeerConnectionManager defaultDiagnostic = new PeerConnectionManager(fixture.client);
        defaultDiagnostic.addDiagnosticGeneratedListener(listener);
        // The default factory is covered through a debug-producing failure.
        defaultDiagnostic.getCachedMessageConnectionAsync("missing").join();
        defaultDiagnostic.close();
    }

    @Test
    void incomingMessageConnectionIsHandedOffStartedAndCached() {
        Fixture fixture = new Fixture();
        ConnectionProbe incoming = ConnectionProbe.connection(DIRECT_ENDPOINT);
        ConnectionProbe message = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        fixture.factory.messageHandoff = message;

        fixture.manager()
                .addOrUpdateMessageConnectionAsync(USERNAME, incoming.connection())
                .join();

        assertEquals(1, incoming.handoffCount);
        assertEquals(1, incoming.closeCount);
        assertEquals(1, message.startReadingCount);
        assertEquals(ConnectionTypes.INBOUND.or(ConnectionTypes.DIRECT), message.type);
        assertSame(
                message.messageConnection(),
                fixture.manager().getCachedMessageConnectionAsync(USERNAME).join());
        assertEquals(
                List.of(new PeerEndpoint(USERNAME, DIRECT_ENDPOINT)),
                fixture.manager().getMessageConnections());
        assertEquals(1, message.messageReadListeners.size());
        assertEquals(1, message.messageReceivedListeners.size());
        assertEquals(1, message.messageWrittenListeners.size());
    }

    @Test
    void incomingMessageSupersedesWithoutDisposingOldConnection() {
        Fixture fixture = new Fixture();
        ConnectionProbe first = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        ConnectionProbe second = ConnectionProbe.message(USERNAME, INDIRECT_ENDPOINT);
        fixture.factory.messageHandoff = first;
        fixture.manager()
                .addOrUpdateMessageConnectionAsync(
                        USERNAME, ConnectionProbe.connection(DIRECT_ENDPOINT).connection())
                .join();

        fixture.factory.messageHandoff = second;
        fixture.manager()
                .addOrUpdateMessageConnectionAsync(
                        USERNAME, ConnectionProbe.connection(INDIRECT_ENDPOINT).connection())
                .join();

        assertEquals(0, first.closeCount);
        assertSame(
                second.messageConnection(),
                fixture.manager().getCachedMessageConnectionAsync(USERNAME).join());
        assertTrue(fixture.diagnostic.contains("Superseding cached"));
    }

    @Test
    void incomingMessageStartFailureClosesAndPurgesCache() {
        Fixture fixture = new Fixture();
        ConnectionProbe message = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        RuntimeException expected = new RuntimeException("start failed");
        message.startFailure = expected;
        fixture.factory.messageHandoff = message;

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> fixture.manager()
                        .addOrUpdateMessageConnectionAsync(
                                USERNAME,
                                ConnectionProbe.connection(DIRECT_ENDPOINT).connection())
                        .join());

        ConnectionException mapped = assertInstanceOf(ConnectionException.class, thrown.getCause());
        assertSame(expected, mapped.getCause());
        assertEquals(1, message.closeCount);
        assertNull(fixture.manager().getCachedMessageConnectionAsync(USERNAME).join());
        assertTrue(fixture.diagnostic.contains("Purging message connection cache"));
    }

    @Test
    void acceptedTransferReadsLittleEndianRemoteToken() {
        Fixture fixture = new Fixture();
        ConnectionProbe incoming = ConnectionProbe.connection(DIRECT_ENDPOINT);
        ConnectionProbe transfer = ConnectionProbe.connection(DIRECT_ENDPOINT);
        transfer.readFuture = CompletableFuture.completedFuture(littleEndian(TOKEN));
        fixture.factory.transferHandoff = transfer;

        TransferConnectionResult result = fixture.manager()
                .getTransferConnectionAsync(USERNAME, 91, incoming.connection())
                .join();

        assertSame(transfer.connection(), result.connection());
        assertEquals(TOKEN, result.remoteToken());
        assertEquals(1, incoming.handoffCount);
        assertEquals(ConnectionTypes.INBOUND.or(ConnectionTypes.DIRECT), transfer.type);
    }

    @Test
    void acceptedTransferReadFailureClosesAndMapsException() {
        Fixture fixture = new Fixture();
        ConnectionProbe transfer = ConnectionProbe.connection(DIRECT_ENDPOINT);
        IllegalStateException expected = new IllegalStateException("read");
        transfer.readFuture = CompletableFuture.failedFuture(expected);
        fixture.factory.transferHandoff = transfer;

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> fixture.manager()
                        .getTransferConnectionAsync(
                                USERNAME,
                                TOKEN,
                                ConnectionProbe.connection(DIRECT_ENDPOINT).connection())
                        .join());

        ConnectionException mapped = assertInstanceOf(ConnectionException.class, thrown.getCause());
        assertSame(expected, mapped.getCause());
        assertEquals(1, transfer.closeCount);
    }

    @Test
    void connectToPeerTransferConnectsPiercesAndReadsToken() {
        Fixture fixture = new Fixture();
        ConnectionProbe transfer = ConnectionProbe.connection(INDIRECT_ENDPOINT);
        transfer.readFuture = CompletableFuture.completedFuture(littleEndian(TOKEN));
        fixture.factory.transferDirect = transfer;
        ConnectToPeerResponse response =
                new ConnectToPeerResponse(USERNAME, Constants.ConnectionType.TRANSFER, INDIRECT_ENDPOINT, 77, true);

        TransferConnectionResult result =
                fixture.manager().getTransferConnectionAsync(response).join();

        assertSame(transfer.connection(), result.connection());
        assertEquals(TOKEN, result.remoteToken());
        assertEquals(1, transfer.connectCount);
        assertArrayEquals(new PierceFirewall(77).toByteArray(), transfer.byteWrites.getFirst());
        assertEquals(ConnectionTypes.INBOUND.or(ConnectionTypes.INDIRECT), transfer.type);
    }

    @Test
    void connectToPeerTransferFailureClosesAndMapsException() {
        Fixture fixture = new Fixture();
        ConnectionProbe transfer = ConnectionProbe.connection(INDIRECT_ENDPOINT);
        IllegalStateException expected = new IllegalStateException("connect");
        transfer.connectFuture = CompletableFuture.failedFuture(expected);
        fixture.factory.transferDirect = transfer;

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> fixture.manager()
                        .getTransferConnectionAsync(new ConnectToPeerResponse(
                                USERNAME, Constants.ConnectionType.TRANSFER, INDIRECT_ENDPOINT, TOKEN, false))
                        .join());

        ConnectionException mapped = assertInstanceOf(ConnectionException.class, thrown.getCause());
        assertSame(expected, mapped.getCause());
        assertEquals(1, transfer.closeCount);
    }

    @Test
    void outboundTransferUsesDirectWinnerAndWritesInitializationAndToken() {
        Fixture fixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.connection(DIRECT_ENDPOINT);
        fixture.factory.transferDirect = direct;
        fixture.waiter.defaultFuture = CompletableFuture.failedFuture(new RuntimeException("indirect"));

        IConnection result = fixture.manager()
                .getTransferConnectionAsync(USERNAME, DIRECT_ENDPOINT, TOKEN, CancellationToken.none())
                .join();

        assertSame(direct.connection(), result);
        assertEquals(ConnectionTypes.OUTBOUND.or(ConnectionTypes.DIRECT), direct.type);
        assertEquals(2, direct.byteWrites.size());
        assertArrayEquals(
                new PeerInit(LOCAL_USER, Constants.ConnectionType.TRANSFER, TOKEN).toByteArray(),
                direct.byteWrites.get(0));
        assertArrayEquals(littleEndian(TOKEN), direct.byteWrites.get(1));
        assertTrue(fixture.diagnostic.contains("established first"));
    }

    @Test
    void outboundTransferUsesIndirectWinnerAndSolicitationHandoff() {
        Fixture fixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.connection(DIRECT_ENDPOINT);
        direct.connectFuture = CompletableFuture.failedFuture(new RuntimeException("direct"));
        ConnectionProbe accepted = ConnectionProbe.connection(INDIRECT_ENDPOINT);
        ConnectionProbe indirect = ConnectionProbe.connection(INDIRECT_ENDPOINT);
        fixture.factory.transferDirect = direct;
        fixture.factory.transferHandoff = indirect;
        fixture.waiter.defaultFuture = CompletableFuture.completedFuture(accepted.connection());

        IConnection result = fixture.manager()
                .getTransferConnectionAsync(USERNAME, DIRECT_ENDPOINT, TOKEN, CancellationToken.none())
                .join();

        assertSame(indirect.connection(), result);
        assertEquals(ConnectionTypes.OUTBOUND.or(ConnectionTypes.INDIRECT), indirect.type);
        assertEquals(1, accepted.handoffCount);
        assertEquals(1, accepted.closeCount);
        assertEquals(1, indirect.byteWrites.size());
        assertArrayEquals(littleEndian(TOKEN), indirect.byteWrites.getFirst());
        ConnectToPeerRequest request =
                assertInstanceOf(ConnectToPeerRequest.class, fixture.server.outgoingWrites.getFirst());
        assertEquals(Constants.ConnectionType.TRANSFER, request.getType());
        assertTrue(fixture.manager().getPendingSolicitations().isEmpty());
    }

    @Test
    void outboundTransferBothFailuresProduceSourceConnectionException() {
        Fixture fixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.connection(DIRECT_ENDPOINT);
        direct.connectFuture = CompletableFuture.failedFuture(new RuntimeException("direct"));
        fixture.factory.transferDirect = direct;
        fixture.waiter.defaultFuture = CompletableFuture.failedFuture(new RuntimeException("indirect"));

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> fixture.manager()
                        .getTransferConnectionAsync(USERNAME, DIRECT_ENDPOINT, TOKEN, CancellationToken.none())
                        .join());

        assertTrue(assertInstanceOf(ConnectionException.class, thrown.getCause())
                .getMessage()
                .contains("direct or indirect transfer connection"));
    }

    @Test
    void connectToPeerMessageConnectsPiercesAndCaches() {
        Fixture fixture = new Fixture();
        ConnectionProbe message = ConnectionProbe.message(USERNAME, INDIRECT_ENDPOINT);
        fixture.factory.messageDirect = message;
        ConnectToPeerResponse response =
                new ConnectToPeerResponse(USERNAME, Constants.ConnectionType.PEER, INDIRECT_ENDPOINT, TOKEN, false);

        IMessageConnection result =
                fixture.manager().getOrAddMessageConnectionAsync(response).join();
        IMessageConnection cached =
                fixture.manager().getOrAddMessageConnectionAsync(response).join();

        assertSame(message.messageConnection(), result);
        assertSame(result, cached);
        assertEquals(1, message.connectCount);
        assertArrayEquals(new PierceFirewall(TOKEN).toByteArray(), message.byteWrites.getFirst());
        assertEquals(ConnectionTypes.INBOUND.or(ConnectionTypes.INDIRECT), message.type);
    }

    @Test
    void connectToPeerMessageFailureClosesPurgesAndMaps() {
        Fixture fixture = new Fixture();
        ConnectionProbe message = ConnectionProbe.message(USERNAME, INDIRECT_ENDPOINT);
        IllegalStateException expected = new IllegalStateException("connect");
        message.connectFuture = CompletableFuture.failedFuture(expected);
        fixture.factory.messageDirect = message;

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> fixture.manager()
                        .getOrAddMessageConnectionAsync(new ConnectToPeerResponse(
                                USERNAME, Constants.ConnectionType.PEER, INDIRECT_ENDPOINT, TOKEN, false))
                        .join());

        ConnectionException mapped = assertInstanceOf(ConnectionException.class, thrown.getCause());
        assertSame(expected, mapped.getCause());
        assertEquals(1, message.closeCount);
        assertNull(fixture.manager().getCachedMessageConnectionAsync(USERNAME).join());
    }

    @Test
    void outboundMessageUsesDirectWinnerAndWritesPeerInit() {
        Fixture fixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        fixture.factory.messageDirect = direct;
        fixture.waiter.defaultFuture = CompletableFuture.failedFuture(new RuntimeException("indirect"));

        IMessageConnection result = fixture.manager()
                .getOrAddMessageConnectionAsync(USERNAME, DIRECT_ENDPOINT, TOKEN, CancellationToken.none())
                .join();

        assertSame(direct.messageConnection(), result);
        assertEquals(ConnectionTypes.OUTBOUND.or(ConnectionTypes.DIRECT), direct.type);
        assertArrayEquals(
                new PeerInit(LOCAL_USER, Constants.ConnectionType.PEER, TOKEN).toByteArray(),
                direct.byteWrites.getFirst());
        assertEquals(0, direct.startReadingCount);
    }

    @Test
    void outboundMessageUsesIndirectWinnerAndStartsReading() {
        Fixture fixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        direct.connectFuture = CompletableFuture.failedFuture(new RuntimeException("direct"));
        ConnectionProbe accepted = ConnectionProbe.connection(INDIRECT_ENDPOINT);
        ConnectionProbe indirect = ConnectionProbe.message(USERNAME, INDIRECT_ENDPOINT);
        fixture.factory.messageDirect = direct;
        fixture.factory.messageHandoff = indirect;
        fixture.waiter.defaultFuture = CompletableFuture.completedFuture(accepted.connection());

        IMessageConnection result = fixture.manager()
                .getOrAddMessageConnectionAsync(USERNAME, DIRECT_ENDPOINT, TOKEN, CancellationToken.none())
                .join();

        assertSame(indirect.messageConnection(), result);
        assertEquals(ConnectionTypes.OUTBOUND.or(ConnectionTypes.INDIRECT), indirect.type);
        assertEquals(1, indirect.startReadingCount);
        assertEquals(0, indirect.byteWrites.size());
        assertTrue(fixture.manager().getPendingSolicitations().isEmpty());
        assertEquals(
                Constants.ConnectionType.PEER,
                assertInstanceOf(ConnectToPeerRequest.class, fixture.server.outgoingWrites.getFirst())
                        .getType());
    }

    @Test
    void outboundMessageFailuresPurgeCacheAndMapException() {
        Fixture fixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        direct.connectFuture = CompletableFuture.failedFuture(new RuntimeException("direct"));
        fixture.factory.messageDirect = direct;
        fixture.waiter.defaultFuture = CompletableFuture.failedFuture(new RuntimeException("indirect"));

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> fixture.manager()
                        .getOrAddMessageConnectionAsync(USERNAME, DIRECT_ENDPOINT, TOKEN, CancellationToken.none())
                        .join());

        assertInstanceOf(ConnectionException.class, thrown.getCause());
        assertNull(fixture.manager().getCachedMessageConnectionAsync(USERNAME).join());
        assertTrue(fixture.diagnostic.contains("Purging message connection cache"));
    }

    @Test
    void messageNegotiationFailureClosesAndPreservesCause() {
        Fixture fixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        IllegalStateException expected = new IllegalStateException("write");
        direct.writeFuture = CompletableFuture.failedFuture(expected);
        fixture.factory.messageDirect = direct;
        fixture.waiter.defaultFuture = CompletableFuture.failedFuture(new RuntimeException("indirect"));

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> fixture.manager()
                        .getOrAddMessageConnectionAsync(USERNAME, DIRECT_ENDPOINT, TOKEN, CancellationToken.none())
                        .join());

        ConnectionException mapped = assertInstanceOf(ConnectionException.class, thrown.getCause());
        assertSame(expected, mapped.getCause());
        assertEquals(1, direct.closeCount);
    }

    @Test
    void concurrentMessageRequestsShareOneInFlightConnection() {
        Fixture fixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        CompletableFuture<Void> connect = new CompletableFuture<>();
        direct.connectFuture = connect;
        fixture.factory.messageDirect = direct;
        fixture.waiter.defaultFuture = CompletableFuture.failedFuture(new RuntimeException("indirect"));

        CompletableFuture<IMessageConnection> first = fixture.manager()
                .getOrAddMessageConnectionAsync(USERNAME, DIRECT_ENDPOINT, TOKEN, CancellationToken.none());
        CompletableFuture<IMessageConnection> second = fixture.manager()
                .getOrAddMessageConnectionAsync(USERNAME, DIRECT_ENDPOINT, TOKEN + 1, CancellationToken.none());
        connect.complete(null);

        assertSame(first.join(), second.join());
        assertEquals(1, fixture.factory.messageDirectCount);
    }

    @Test
    void awaitingTransferReturnsEitherWinnerAndMapsDualFailure() {
        Fixture directFixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.connection(DIRECT_ENDPOINT);
        directFixture.waiter.put(
                new WaitKey(Constants.WaitKey.DIRECT_TRANSFER, USERNAME, TOKEN),
                CompletableFuture.completedFuture(direct.connection()));
        directFixture.waiter.put(
                new WaitKey(Constants.WaitKey.INDIRECT_TRANSFER, USERNAME, "file", TOKEN),
                CompletableFuture.failedFuture(new RuntimeException()));
        assertSame(
                direct.connection(),
                directFixture
                        .manager()
                        .awaitTransferConnectionAsync(USERNAME, "file", TOKEN, CancellationToken.none())
                        .join());

        Fixture indirectFixture = new Fixture();
        ConnectionProbe indirect = ConnectionProbe.connection(INDIRECT_ENDPOINT);
        indirectFixture.waiter.put(
                new WaitKey(Constants.WaitKey.DIRECT_TRANSFER, USERNAME, TOKEN),
                CompletableFuture.failedFuture(new RuntimeException()));
        indirectFixture.waiter.put(
                new WaitKey(Constants.WaitKey.INDIRECT_TRANSFER, USERNAME, "file", TOKEN),
                CompletableFuture.completedFuture(indirect.connection()));
        assertSame(
                indirect.connection(),
                indirectFixture
                        .manager()
                        .awaitTransferConnectionAsync(USERNAME, "file", TOKEN, CancellationToken.none())
                        .join());

        Fixture failedFixture = new Fixture();
        failedFixture.waiter.defaultFuture = CompletableFuture.failedFuture(new RuntimeException());
        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> failedFixture
                        .manager()
                        .awaitTransferConnectionAsync(USERNAME, "file", TOKEN, CancellationToken.none())
                        .join());
        assertInstanceOf(ConnectionException.class, thrown.getCause());
    }

    @Test
    void invalidationDisconnectAndRemoveAllCleanUpCache() {
        Fixture fixture = new Fixture();
        ConnectionProbe first = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        fixture.factory.messageHandoff = first;
        fixture.manager()
                .addOrUpdateMessageConnectionAsync(
                        USERNAME, ConnectionProbe.connection(DIRECT_ENDPOINT).connection())
                .join();
        assertTrue(fixture.manager().tryInvalidateMessageConnectionCache(USERNAME));
        assertFalse(fixture.manager().tryInvalidateMessageConnectionCache(USERNAME));

        ConnectionProbe second = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        fixture.factory.messageHandoff = second;
        fixture.manager()
                .addOrUpdateMessageConnectionAsync(
                        USERNAME, ConnectionProbe.connection(DIRECT_ENDPOINT).connection())
                .join();
        second.fireDisconnected("closed", null);
        assertNull(fixture.manager().getCachedMessageConnectionAsync(USERNAME).join());
        assertEquals(1, second.closeCount);
        assertTrue(fixture.diagnostic.contains("Removed message connection record"));

        ConnectionProbe third = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        fixture.factory.messageHandoff = third;
        fixture.manager()
                .addOrUpdateMessageConnectionAsync(
                        USERNAME, ConnectionProbe.connection(DIRECT_ENDPOINT).connection())
                .join();
        fixture.manager().removeAndDisposeAll();
        assertTrue(fixture.manager().getMessageConnections().isEmpty());
        assertEquals(1, third.closeCount);
    }

    @Test
    void returnedSnapshotsAreImmutable() {
        Fixture fixture = new Fixture();
        assertThrows(
                UnsupportedOperationException.class,
                () -> fixture.manager().getPendingSolicitations().put(1, "x"));
        assertThrows(
                UnsupportedOperationException.class,
                () -> fixture.manager().getMessageConnections().add(new PeerEndpoint(USERNAME, DIRECT_ENDPOINT)));
    }

    private static InetSocketAddress endpoint(int port) {
        try {
            return new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port);
        } catch (Exception exception) {
            throw new AssertionError(exception);
        }
    }

    private static byte[] littleEndian(int value) {
        return ByteBuffer.allocate(4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(value)
                .array();
    }

    private static final class Fixture {
        private final RecordingDiagnostic diagnostic = new RecordingDiagnostic();
        private final FakeWaiter waiter = new FakeWaiter();
        private final ConnectionProbe server = ConnectionProbe.message("", endpoint(2242));
        private final FakeFactory factory = new FakeFactory();
        private final FakeClient client = new FakeClient(waiter, server.messageConnection());
        private final PeerConnectionManager manager = new PeerConnectionManager(client, factory, diagnostic);

        private PeerConnectionManager manager() {
            return manager;
        }
    }

    private static final class FakeClient implements PeerConnectionManagerClient {
        private final FakeWaiter waiter;
        private final IMessageConnection server;
        private final AtomicInteger token = new AtomicInteger(TOKEN);
        private final IPeerMessageHandler handler = (IPeerMessageHandler) Proxy.newProxyInstance(
                IPeerMessageHandler.class.getClassLoader(),
                new Class<?>[] {IPeerMessageHandler.class},
                (proxy, method, arguments) -> defaultValue(method.getReturnType()));

        private FakeClient(FakeWaiter waiter, IMessageConnection server) {
            this.waiter = waiter;
            this.server = server;
        }

        @Override
        public SoulseekClientOptions getOptions() {
            return new SoulseekClientOptions();
        }

        @Override
        public String getUsername() {
            return LOCAL_USER;
        }

        @Override
        public int getNextToken() {
            return token.getAndIncrement();
        }

        @Override
        public IWaiter getWaiter() {
            return waiter;
        }

        @Override
        public IMessageConnection getServerConnection() {
            return server;
        }

        @Override
        public IPeerMessageHandler getPeerMessageHandler() {
            return handler;
        }
    }

    private static final class FakeFactory implements IConnectionFactory {
        private ConnectionProbe messageDirect;
        private ConnectionProbe messageHandoff;
        private ConnectionProbe transferDirect;
        private ConnectionProbe transferHandoff;
        private int messageDirectCount;

        @Override
        public IMessageConnection getDistributedConnection(
                String username, InetSocketAddress ipEndPoint, ConnectionOptions options, ITcpClient tcpClient) {
            throw new AssertionError("unexpected distributed connection");
        }

        @Override
        public IMessageConnection getMessageConnection(
                String username, InetSocketAddress ipEndPoint, ConnectionOptions options, ITcpClient tcpClient) {
            if (tcpClient != null) {
                assertNotNull(messageHandoff);
                return messageHandoff.messageConnection();
            }
            messageDirectCount++;
            assertNotNull(messageDirect);
            return messageDirect.messageConnection();
        }

        @Override
        public IMessageConnection getServerConnection(
                InetSocketAddress ipEndPoint,
                ConnectionEventListener<Void> connectedEventHandler,
                ConnectionEventListener<ConnectionDisconnectedEventArgs> disconnectedEventHandler,
                MessageConnectionEventListener<MessageEventArgs> messageReadEventHandler,
                MessageConnectionEventListener<MessageEventArgs> messageWrittenEventHandler,
                ConnectionOptions options,
                ITcpClient tcpClient) {
            throw new AssertionError("unexpected server connection");
        }

        @Override
        public IConnection getTransferConnection(
                InetSocketAddress ipEndPoint, ConnectionOptions options, ITcpClient tcpClient) {
            if (tcpClient != null) {
                assertNotNull(transferHandoff);
                return transferHandoff.connection();
            }
            assertNotNull(transferDirect);
            return transferDirect.connection();
        }
    }

    private static final class FakeWaiter implements IWaiter {
        private final Map<WaitKey, CompletableFuture<?>> futures = new HashMap<>();
        private CompletableFuture<?> defaultFuture =
                CompletableFuture.failedFuture(new IllegalStateException("No configured wait"));

        private void put(WaitKey key, CompletableFuture<?> future) {
            futures.put(key, future);
        }

        @Override
        public int getDefaultTimeout() {
            return 5_000;
        }

        @Override
        public void cancel(WaitKey key) {
            CompletableFuture<?> future = futures.get(key);
            if (future != null) {
                future.cancel(false);
            }
        }

        @Override
        public void cancelAll() {
            futures.values().forEach(future -> future.cancel(false));
        }

        @Override
        public void complete(WaitKey key) {
            complete(key, null);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> void complete(WaitKey key, T result) {
            CompletableFuture<T> future = (CompletableFuture<T>) futures.get(key);
            if (future != null) {
                future.complete(result);
            }
        }

        @Override
        public boolean hasWait(WaitKey key) {
            return futures.containsKey(key);
        }

        @Override
        public void fail(WaitKey key, Throwable exception) {
            CompletableFuture<?> future = futures.get(key);
            if (future != null) {
                future.completeExceptionally(exception);
            }
        }

        @Override
        public void timeout(WaitKey key) {
            fail(key, new java.util.concurrent.TimeoutException());
        }

        @Override
        public CompletableFuture<Void> waitAsync(WaitKey key) {
            return waitAsync(key, Void.class, null, CancellationToken.none());
        }

        @Override
        public CompletableFuture<Void> waitAsync(WaitKey key, Integer timeout) {
            return waitAsync(key, Void.class, timeout, CancellationToken.none());
        }

        @Override
        public CompletableFuture<Void> waitAsync(WaitKey key, Integer timeout, CancellationToken cancellationToken) {
            return waitAsync(key, Void.class, timeout, cancellationToken);
        }

        @Override
        public <T> CompletableFuture<T> waitAsync(WaitKey key, Class<T> resultType) {
            return waitAsync(key, resultType, null, CancellationToken.none());
        }

        @Override
        public <T> CompletableFuture<T> waitAsync(WaitKey key, Class<T> resultType, Integer timeout) {
            return waitAsync(key, resultType, timeout, CancellationToken.none());
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> CompletableFuture<T> waitAsync(
                WaitKey key, Class<T> resultType, Integer timeout, CancellationToken cancellationToken) {
            return (CompletableFuture<T>) futures.getOrDefault(key, defaultFuture);
        }

        @Override
        public CompletableFuture<Void> waitIndefinitelyAsync(WaitKey key) {
            return waitAsync(key);
        }

        @Override
        public CompletableFuture<Void> waitIndefinitelyAsync(WaitKey key, CancellationToken cancellationToken) {
            return waitAsync(key, null, cancellationToken);
        }

        @Override
        public <T> CompletableFuture<T> waitIndefinitelyAsync(WaitKey key, Class<T> resultType) {
            return waitAsync(key, resultType);
        }

        @Override
        public <T> CompletableFuture<T> waitIndefinitelyAsync(
                WaitKey key, Class<T> resultType, CancellationToken cancellationToken) {
            return waitAsync(key, resultType, null, cancellationToken);
        }

        @Override
        public void close() {
            cancelAll();
        }
    }

    private static final class RecordingDiagnostic implements IDiagnosticFactory {
        private final List<String> messages = new ArrayList<>();

        private boolean contains(String value) {
            return messages.stream().anyMatch(message -> message.toLowerCase().contains(value.toLowerCase()));
        }

        @Override
        public void trace(String message) {
            messages.add(message);
        }

        @Override
        public void trace(String message, Throwable exception) {
            messages.add(message);
        }

        @Override
        public void debug(String message) {
            messages.add(message);
        }

        @Override
        public void debug(String message, Throwable exception) {
            messages.add(message);
        }

        @Override
        public void info(String message) {
            messages.add(message);
        }

        @Override
        public void warning(String message) {
            messages.add(message);
        }

        @Override
        public void warning(String message, Throwable exception) {
            messages.add(message);
        }
    }

    private static final class ConnectionProbe implements InvocationHandler {
        private final boolean message;
        private final String username;
        private final InetSocketAddress endpoint;
        private final UUID id = UUID.randomUUID();
        private final ITcpClient tcpClient = (ITcpClient) Proxy.newProxyInstance(
                ITcpClient.class.getClassLoader(),
                new Class<?>[] {ITcpClient.class},
                (proxy, method, arguments) -> defaultValue(method.getReturnType()));
        private final IConnection proxy;
        private final List<ConnectionEventListener<ConnectionDisconnectedEventArgs>> disconnectedListeners =
                new ArrayList<>();
        private final List<MessageConnectionEventListener<MessageEventArgs>> messageReadListeners = new ArrayList<>();
        private final List<MessageConnectionEventListener<MessageReceivedEventArgs>> messageReceivedListeners =
                new ArrayList<>();
        private final List<MessageConnectionEventListener<MessageEventArgs>> messageWrittenListeners =
                new ArrayList<>();
        private final List<byte[]> byteWrites = new ArrayList<>();
        private final List<IOutgoingMessage> outgoingWrites = new ArrayList<>();
        private ConnectionTypes type = ConnectionTypes.NONE;
        private CompletableFuture<Void> connectFuture = CompletableFuture.completedFuture(null);
        private CompletableFuture<Void> writeFuture = CompletableFuture.completedFuture(null);
        private CompletableFuture<byte[]> readFuture = CompletableFuture.completedFuture(new byte[4]);
        private RuntimeException startFailure;
        private int closeCount;
        private int connectCount;
        private int handoffCount;
        private int startReadingCount;

        private ConnectionProbe(boolean message, String username, InetSocketAddress endpoint) {
            this.message = message;
            this.username = username;
            this.endpoint = endpoint;
            Class<?>[] interfaces =
                    message ? new Class<?>[] {IMessageConnection.class} : new Class<?>[] {IConnection.class};
            proxy = (IConnection) Proxy.newProxyInstance(IConnection.class.getClassLoader(), interfaces, this);
        }

        private static ConnectionProbe connection(InetSocketAddress endpoint) {
            return new ConnectionProbe(false, null, endpoint);
        }

        private static ConnectionProbe message(String username, InetSocketAddress endpoint) {
            return new ConnectionProbe(true, username, endpoint);
        }

        private IConnection connection() {
            return proxy;
        }

        private IMessageConnection messageConnection() {
            if (!message) {
                throw new IllegalStateException("Not a message connection");
            }
            return (IMessageConnection) proxy;
        }

        private void fireDisconnected(String message, Exception exception) {
            ConnectionDisconnectedEventArgs eventArgs = new ConnectionDisconnectedEventArgs(message, exception);
            List.copyOf(disconnectedListeners).forEach(listener -> listener.handle(proxy, eventArgs));
        }

        @Override
        public Object invoke(Object ignored, Method method, Object[] arguments) {
            String name = method.getName();
            return switch (name) {
                case "getId" -> id;
                case "getInactiveTime" -> Duration.ZERO;
                case "getIpEndPoint" -> endpoint;
                case "getKey" -> new ConnectionKey(username, endpoint);
                case "getOptions" -> new ConnectionOptions();
                case "getState" -> ConnectionState.CONNECTED;
                case "getType" -> type;
                case "setType" -> {
                    type = (ConnectionTypes) arguments[0];
                    yield null;
                }
                case "getWriteQueueDepth" -> 0;
                case "getCodeLength" -> 4;
                case "isServerConnection" -> username == null || username.isEmpty();
                case "isReadingContinuously" -> startReadingCount > 0;
                case "getUsername" -> username == null ? "" : username;
                case "connectAsync" -> {
                    connectCount++;
                    yield connectFuture;
                }
                case "readAsync" -> readFuture;
                case "writeAsync" -> {
                    if (arguments[0] instanceof byte[] bytes) {
                        byteWrites.add(Arrays.copyOf(bytes, bytes.length));
                    } else if (arguments[0] instanceof IOutgoingMessage value) {
                        outgoingWrites.add(value);
                    }
                    yield writeFuture;
                }
                case "handoffTcpClient" -> {
                    handoffCount++;
                    yield tcpClient;
                }
                case "startReadingContinuously" -> {
                    startReadingCount++;
                    if (startFailure != null) {
                        throw startFailure;
                    }
                    yield null;
                }
                case "close" -> {
                    closeCount++;
                    yield null;
                }
                case "disconnect" -> null;
                case "addDisconnectedListener" -> {
                    disconnectedListeners.add(cast(arguments[0]));
                    yield null;
                }
                case "removeDisconnectedListener" -> {
                    disconnectedListeners.remove(arguments[0]);
                    yield null;
                }
                case "addMessageReadListener" -> {
                    messageReadListeners.add(cast(arguments[0]));
                    yield null;
                }
                case "removeMessageReadListener" -> {
                    messageReadListeners.remove(arguments[0]);
                    yield null;
                }
                case "addMessageReceivedListener" -> {
                    messageReceivedListeners.add(cast(arguments[0]));
                    yield null;
                }
                case "removeMessageReceivedListener" -> {
                    messageReceivedListeners.remove(arguments[0]);
                    yield null;
                }
                case "addMessageWrittenListener" -> {
                    messageWrittenListeners.add(cast(arguments[0]));
                    yield null;
                }
                case "removeMessageWrittenListener" -> {
                    messageWrittenListeners.remove(arguments[0]);
                    yield null;
                }
                case "toString" -> "ConnectionProbe(" + endpoint + ")";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == arguments[0];
                default -> defaultValue(method.getReturnType());
            };
        }

        @SuppressWarnings("unchecked")
        private static <T> T cast(Object value) {
            return (T) value;
        }
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0f;
        }
        if (type == double.class) {
            return 0d;
        }
        if (type == char.class) {
            return '\0';
        }
        return null;
    }
}
