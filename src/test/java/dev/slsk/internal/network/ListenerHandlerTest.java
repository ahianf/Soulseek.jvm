// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.CacheLookupResult;
import dev.slsk.internal.SearchResponseCache;
import dev.slsk.internal.SearchResponseCacheRecord;
import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.DefaultWaiter;
import dev.slsk.internal.common.Outcomes;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.diagnostics.DiagnosticEvent;
import dev.slsk.internal.diagnostics.DiagnosticLevel;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.messaging.messages.PeerInit;
import dev.slsk.internal.messaging.messages.PierceFirewall;
import dev.slsk.internal.network.tcp.Connection;
import dev.slsk.internal.network.tcp.Listener;
import dev.slsk.internal.network.tcp.ListenerAcceptedEventListener;
import dev.slsk.internal.options.ConnectionOptions;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.search.SearchResponder;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ListenerHandlerTest {
    @Test
    void constructorValidatesClientAndDefaultDiagnosticRaisesEvents() throws Exception {
        assertThrows(NullPointerException.class, () -> new DefaultListenerHandler(null));
        try (Fixture fixture = fixture(null)) {
            DefaultListenerHandler handler = new DefaultListenerHandler(fixture.client);
            AtomicReference<DiagnosticEvent> event = new AtomicReference<>();
            handler.addDiagnosticGeneratedListener((sender, args) -> event.set(args));
            handler.getDiagnostic().info("test");
            assertEquals("test", event.get().getMessage());
        }
    }

    @Test
    void readFailureAndUnrecognizedMessageDisconnectAndClose() throws Exception {
        try (Fixture fixture = fixture(null)) {
            ConnectionProbe readFailure = ConnectionProbe.failure(new RuntimeException("read"));
            fixture.handler.handleConnectionAsync(readFailure.proxy).join();
            assertNotNull(readFailure.disconnectedException);
            assertTrue(readFailure.closed);
            assertTrue(fixture.diagnostic.debug.stream().anyMatch(text -> text.contains("Failed to initialize")));

            ConnectionProbe unknown = ConnectionProbe.message(new byte[] {1});
            fixture.handler.handleConnectionAsync(unknown.proxy).join();
            assertTrue(unknown.disconnectedException.getMessage().contains("Unrecognized initialization message"));
            assertTrue(unknown.closed);
        }
    }

    @Test
    void peerAndDistributedInitializationsRouteConnections() throws Exception {
        try (Fixture fixture = fixture(null)) {
            ConnectionProbe peer =
                    ConnectionProbe.message(new PeerInit("alice", Constants.ConnectionType.PEER, 1).toByteArray());
            fixture.handler.handleConnectionAsync(peer.proxy).join();
            assertEquals("alice", fixture.peer.addedUsername);
            assertSame(peer.proxy, fixture.peer.addedConnection);

            ConnectionProbe distributed =
                    ConnectionProbe.message(new PeerInit("bob", Constants.ConnectionType.DISTRIBUTED, 2).toByteArray());
            fixture.handler.handleConnectionAsync(distributed.proxy).join();
            assertEquals("bob", fixture.distributed.addedUsername);
            assertSame(distributed.proxy, fixture.distributed.addedConnection);
        }
    }

    @Test
    void expectedTransferCompletesDirectWait() throws Exception {
        try (Fixture fixture = fixture(null)) {
            Connection transferConnection = ConnectionProbe.message(new byte[] {1}).proxy;
            fixture.peer.transferResult = new TransferConnectionResult(transferConnection, 24);
            WaitKey key = new WaitKey(Constants.WaitKey.DIRECT_TRANSFER, "alice", 24);
            Wait<Connection> wait = fixture.waiter.register(key, Connection.class, -1, null);
            ConnectionProbe incoming =
                    ConnectionProbe.message(new PeerInit("alice", Constants.ConnectionType.TRANSFER, 7).toByteArray());

            fixture.handler.handleConnectionAsync(incoming.proxy).join();

            assertSame(transferConnection, wait.await());
            assertEquals(7, fixture.peer.transferToken);
            assertSame(incoming.proxy, fixture.peer.transferIncoming);
        }
    }

    @Test
    void unexpectedTransferIsRejected() throws Exception {
        try (Fixture fixture = fixture(null)) {
            ConnectionProbe transfer = ConnectionProbe.message(new byte[] {1});
            fixture.peer.transferResult = new TransferConnectionResult(transfer.proxy, 24);
            ConnectionProbe incoming =
                    ConnectionProbe.message(new PeerInit("alice", Constants.ConnectionType.TRANSFER, 7).toByteArray());

            fixture.handler.handleConnectionAsync(incoming.proxy).join();

            assertEquals("Transfer connection rejected: unknown token", transfer.disconnectMessage);
        }
    }

    @Test
    void peerAndDistributedPierceFirewallCompleteSolicitedWaits() throws Exception {
        try (Fixture fixture = fixture(null)) {
            fixture.peer.pending = Map.of(8, "alice");
            WaitKey peerKey = new WaitKey(Constants.WaitKey.SOLICITED_PEER_CONNECTION, "alice", 8);
            Wait<Connection> peerWait = fixture.waiter.register(peerKey, Connection.class, -1, null);
            ConnectionProbe peer = ConnectionProbe.message(new PierceFirewall(8).toByteArray());
            fixture.handler.handleConnectionAsync(peer.proxy).join();
            assertSame(peer.proxy, peerWait.await());

            fixture.peer.pending = Map.of();
            fixture.distributed.pending = Map.of(9, "bob");
            WaitKey distributedKey = new WaitKey(Constants.WaitKey.SOLICITED_DISTRIBUTED_CONNECTION, "bob", 9);
            Wait<Connection> distributedWait = fixture.waiter.register(distributedKey, Connection.class, -1, null);
            ConnectionProbe distributed = ConnectionProbe.message(new PierceFirewall(9).toByteArray());
            fixture.handler.handleConnectionAsync(distributed.proxy).join();
            assertSame(distributed.proxy, distributedWait.await());
        }
    }

    @Test
    void cachedSearchPierceAddsConnectionThenResponds() throws Exception {
        TestCache cache = new TestCache();
        cache.lookup = CacheLookupResult.found(new SearchResponseCacheRecord("alice", 1, "query", null));
        try (Fixture fixture = fixture(cache)) {
            ConnectionProbe connection = ConnectionProbe.message(new PierceFirewall(11).toByteArray());

            fixture.handler.handleConnectionAsync(connection.proxy).join();

            assertEquals("alice", fixture.peer.addedUsername);
            assertSame(connection.proxy, fixture.peer.addedConnection);
            assertEquals(11, fixture.searchResponder.responseToken);
            assertEquals(11, cache.lastLookupToken);
        }
    }

    @Test
    void unknownPierceFirewallDisconnectsAndDisposes() throws Exception {
        try (Fixture fixture = fixture(null)) {
            ConnectionProbe connection = ConnectionProbe.message(new PierceFirewall(12).toByteArray());

            fixture.handler.handleConnectionAsync(connection.proxy).join();

            assertTrue(connection
                    .disconnectedException
                    .getMessage()
                    .contains("Unknown PierceFirewall attempt with token 12"));
            assertTrue(connection.closed);
        }
    }

    @Test
    void unknownPeerInitTypeIsIgnoredLikeSource() throws Exception {
        try (Fixture fixture = fixture(null)) {
            ConnectionProbe connection = ConnectionProbe.message(new PeerInit("alice", "X", 1).toByteArray());

            fixture.handler.handleConnectionAsync(connection.proxy).join();

            assertNull(connection.disconnectedException);
            assertFalse(connection.closed);
            assertNull(fixture.peer.addedUsername);
            assertNull(fixture.distributed.addedUsername);
        }
    }

    private static Fixture fixture(TestCache cache) throws Exception {
        PeerProbe peer = new PeerProbe();
        DistributedProbe distributed = new DistributedProbe();
        SearchResponderProbe search = new SearchResponderProbe();
        DefaultWaiter waiter = new DefaultWaiter();
        SoulseekClientOptions options = options(cache);
        TestListener listener = new TestListener();
        TestClient client = new TestClient(options, listener, peer.proxy, distributed.proxy, waiter, search.proxy);
        RecordingDiagnostic diagnostic = new RecordingDiagnostic();
        return new Fixture(
                client, peer, distributed, search, waiter, diagnostic, new DefaultListenerHandler(client, diagnostic));
    }

    private static SoulseekClientOptions options(SearchResponseCache cache) {
        return new SoulseekClientOptions(
                true,
                null,
                50_000,
                true,
                true,
                25,
                2,
                10,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                true,
                5_000,
                true,
                true,
                false,
                DiagnosticLevel.TRACE,
                0,
                null,
                null,
                null,
                null,
                null,
                null,
                cache,
                false);
    }

    private record Fixture(
            TestClient client,
            PeerProbe peer,
            DistributedProbe distributed,
            SearchResponderProbe searchResponder,
            DefaultWaiter waiter,
            RecordingDiagnostic diagnostic,
            DefaultListenerHandler handler)
            implements AutoCloseable {
        @Override
        public void close() {
            waiter.close();
        }
    }

    private record TestClient(
            SoulseekClientOptions options,
            Listener listener,
            PeerConnectionManager peerConnectionManager,
            DistributedConnectionManager distributedConnectionManager,
            DefaultWaiter waiter,
            SearchResponder searchResponder)
            implements ListenerHandlerClient {
        @Override
        public SoulseekClientOptions getOptions() {
            return options;
        }

        @Override
        public Listener getListener() {
            return listener;
        }

        @Override
        public PeerConnectionManager getPeerConnectionManager() {
            return peerConnectionManager;
        }

        @Override
        public DistributedConnectionManager getDistributedConnectionManager() {
            return distributedConnectionManager;
        }

        @Override
        public Waiter getWaiter() {
            return waiter;
        }

        @Override
        public SearchResponder getSearchResponder() {
            return searchResponder;
        }
    }

    private static final class TestListener implements Listener {
        @Override
        public void addAcceptedListener(ListenerAcceptedEventListener listener) {}

        @Override
        public void removeAcceptedListener(ListenerAcceptedEventListener listener) {}

        @Override
        public ConnectionOptions getConnectionOptions() {
            return new ConnectionOptions();
        }

        @Override
        public InetAddress getIpAddress() {
            return InetAddress.getLoopbackAddress();
        }

        @Override
        public boolean isListening() {
            return true;
        }

        @Override
        public int getPort() {
            return 50_000;
        }

        @Override
        public void start() {}

        @Override
        public void stop() {}
    }

    private static final class ConnectionProbe {
        private final Connection proxy;
        private final ArrayDeque<CompletableFuture<byte[]>> reads = new ArrayDeque<>();
        private String disconnectMessage;
        private Exception disconnectedException;
        private boolean closed;

        private ConnectionProbe() {
            InetSocketAddress endpoint = new InetSocketAddress("127.0.0.1", 1234);
            UUID id = UUID.randomUUID();
            proxy = (Connection) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[] {Connection.class}, (ignored, method, arguments) -> {
                        return switch (method.getName()) {
                            case "getIpEndpoint" -> endpoint;
                            case "getId" -> id;
                            case "read" -> Outcomes.raise(reads.removeFirst());
                            case "disconnect" -> {
                                disconnectMessage =
                                        arguments == null || arguments.length == 0 ? null : (String) arguments[0];
                                disconnectedException =
                                        arguments != null && arguments.length > 1 ? (Exception) arguments[1] : null;
                                yield null;
                            }
                            case "close" -> {
                                closed = true;
                                yield null;
                            }
                            case "toString" -> "connection";
                            default -> defaultValue(method.getReturnType());
                        };
                    });
        }

        static ConnectionProbe message(byte[] fullMessage) {
            ConnectionProbe probe = new ConnectionProbe();
            byte[] body;
            if (fullMessage.length >= 4
                    && ByteBuffer.wrap(fullMessage)
                                    .order(ByteOrder.LITTLE_ENDIAN)
                                    .getInt()
                            == fullMessage.length - 4) {
                body = java.util.Arrays.copyOfRange(fullMessage, 4, fullMessage.length);
            } else {
                body = fullMessage;
            }
            probe.reads.add(CompletableFuture.completedFuture(ByteBuffer.allocate(4)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putInt(body.length)
                    .array()));
            probe.reads.add(CompletableFuture.completedFuture(body));
            return probe;
        }

        static ConnectionProbe failure(Throwable failure) {
            ConnectionProbe probe = new ConnectionProbe();
            probe.reads.add(CompletableFuture.failedFuture(failure));
            return probe;
        }
    }

    private static final class PeerProbe {
        private Map<Integer, String> pending = Map.of();
        private String addedUsername;
        private Connection addedConnection;
        private int transferToken;
        private Connection transferIncoming;
        private TransferConnectionResult transferResult;
        private final PeerConnectionManager proxy = (PeerConnectionManager) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {PeerConnectionManager.class},
                (ignored, method, arguments) -> {
                    return switch (method.getName()) {
                        case "getPendingSolicitations" -> pending;
                        case "addOrUpdateMessageConnection" -> {
                            addedUsername = (String) arguments[0];
                            addedConnection = (Connection) arguments[1];
                            yield null;
                        }
                        case "getTransferConnection" -> {
                            transferToken = (Integer) arguments[1];
                            transferIncoming = (Connection) arguments[2];
                            yield transferResult;
                        }
                        case "toString" -> "peerManager";
                        default -> defaultValue(method.getReturnType());
                    };
                });
    }

    private static final class DistributedProbe {
        private Map<Integer, String> pending = Map.of();
        private String addedUsername;
        private Connection addedConnection;
        private final DistributedConnectionManager proxy = (DistributedConnectionManager) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {DistributedConnectionManager.class},
                (ignored, method, arguments) -> {
                    return switch (method.getName()) {
                        case "getPendingSolicitations" -> pending;
                        case "addOrUpdateChildConnectionAsync" -> {
                            addedUsername = (String) arguments[0];
                            addedConnection = (Connection) arguments[1];
                            yield CompletableFuture.completedFuture(null);
                        }
                        case "toString" -> "distributedManager";
                        default -> defaultValue(method.getReturnType());
                    };
                });
    }

    private static final class SearchResponderProbe {
        private int responseToken;
        private final SearchResponder proxy = (SearchResponder) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {SearchResponder.class}, (ignored, method, arguments) -> {
                    if (method.getName().equals("tryRespondAsync") && arguments.length == 1) {
                        responseToken = (Integer) arguments[0];
                        return CompletableFuture.completedFuture(true);
                    }
                    return defaultValue(method.getReturnType());
                });
    }

    private static final class TestCache implements SearchResponseCache {
        private CacheLookupResult<SearchResponseCacheRecord> lookup = CacheLookupResult.notFound();
        private int lastLookupToken;

        @Override
        public void put(int responseToken, SearchResponseCacheRecord response) {}

        @Override
        public CacheLookupResult<SearchResponseCacheRecord> lookup(int responseToken) {
            lastLookupToken = responseToken;
            return lookup;
        }

        @Override
        public CacheLookupResult<SearchResponseCacheRecord> remove(int responseToken) {
            return CacheLookupResult.notFound();
        }
    }

    private static final class RecordingDiagnostic implements DiagnosticSink {
        private final java.util.ArrayList<String> debug = new java.util.ArrayList<>();

        @Override
        public void trace(String message) {}

        @Override
        public void trace(String message, Throwable exception) {}

        @Override
        public void debug(String message) {
            debug.add(message);
        }

        @Override
        public void debug(String message, Throwable exception) {
            debug.add(message);
        }

        @Override
        public void info(String message) {}

        @Override
        public void warning(String message) {}

        @Override
        public void warning(String message, Throwable exception) {}
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
        if (type == double.class) {
            return 0d;
        }
        return null;
    }
}
