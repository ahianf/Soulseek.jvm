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
import dev.slsk.SoulseekClientStates;
import dev.slsk.common.Constants;
import dev.slsk.common.WaitKey;
import dev.slsk.common.Waiter;
import dev.slsk.diagnostics.IDiagnosticFactory;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.messaging.handlers.DistributedMessageHandler;
import dev.slsk.messaging.messages.AcceptChildrenCommand;
import dev.slsk.messaging.messages.BranchLevelCommand;
import dev.slsk.messaging.messages.BranchRootCommand;
import dev.slsk.messaging.messages.ConnectToPeerRequest;
import dev.slsk.messaging.messages.ConnectToPeerResponse;
import dev.slsk.messaging.messages.DistributedBranchLevel;
import dev.slsk.messaging.messages.DistributedBranchRoot;
import dev.slsk.messaging.messages.DistributedSearchRequest;
import dev.slsk.messaging.messages.HaveNoParentsCommand;
import dev.slsk.messaging.messages.OutgoingMessage;
import dev.slsk.messaging.messages.PeerInit;
import dev.slsk.messaging.messages.PierceFirewall;
import dev.slsk.network.tcp.Connection;
import dev.slsk.network.tcp.ConnectionDisconnectedEventArgs;
import dev.slsk.network.tcp.ConnectionEventListener;
import dev.slsk.network.tcp.ConnectionKey;
import dev.slsk.network.tcp.ConnectionState;
import dev.slsk.network.tcp.ConnectionTypes;
import dev.slsk.network.tcp.TcpClient;
import dev.slsk.options.ConnectionOptions;
import dev.slsk.options.SoulseekClientOptions;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DistributedConnectionManagerTest {
    private static final String LOCAL_USER = "local";
    private static final String USERNAME = "peer";
    private static final InetSocketAddress ENDPOINT = endpoint(42001);
    private static final int TOKEN = 0x13572468;

    private final List<DefaultDistributedConnectionManager> managers = new ArrayList<>();

    @AfterEach
    void closeManagers() {
        managers.forEach(DefaultDistributedConnectionManager::close);
    }

    @Test
    void constructionAndInitialPropertiesMatchSource() {
        assertThrows(NullPointerException.class, () -> new DefaultDistributedConnectionManager(null));
        Fixture fixture = fixture();
        DefaultDistributedConnectionManager manager = fixture.manager;

        assertNull(manager.getAverageBroadcastLatency());
        assertEquals(0, manager.getBranchLevel());
        assertEquals(LOCAL_USER, manager.getBranchRoot());
        assertFalse(manager.hasParent());
        assertFalse(manager.isBranchRoot());
        assertFalse(manager.canAcceptChildren());
        assertEquals(25, manager.getChildLimit());
        assertEquals(new PeerEndpoint("", null), manager.getParent());
        assertTrue(manager.getChildren().isEmpty());
        assertTrue(manager.getPendingSolicitations().isEmpty());
        assertDoesNotThrow(manager::close);
        assertDoesNotThrow(manager::close);
    }

    @Test
    void promotionAndDemotionRaiseEventsOnce() {
        Fixture fixture = fixture();
        AtomicInteger promoted = new AtomicInteger();
        AtomicInteger demoted = new AtomicInteger();
        AtomicInteger states = new AtomicInteger();
        fixture.manager.addPromotedToBranchRootListener((sender, args) -> promoted.incrementAndGet());
        fixture.manager.addDemotedFromBranchRootListener((sender, args) -> demoted.incrementAndGet());
        fixture.manager.addStateChangedListener((sender, args) -> states.incrementAndGet());

        fixture.manager.promoteToBranchRoot();
        fixture.manager.promoteToBranchRoot();
        assertTrue(fixture.manager.isBranchRoot());
        assertTrue(fixture.manager.canAcceptChildren());
        assertEquals(1, promoted.get());

        fixture.manager.demoteFromBranchRoot();
        fixture.manager.demoteFromBranchRoot();
        assertFalse(fixture.manager.isBranchRoot());
        assertEquals(1, demoted.get());
        assertEquals(2, states.get());
    }

    @Test
    void rejectedDirectChildIsClosedAndStatusIsUpdated() {
        Fixture fixture = fixture();
        ConnectionProbe incoming = ConnectionProbe.connection(ENDPOINT);

        fixture.manager
                .addOrUpdateChildConnectionAsync(USERNAME, incoming.connection())
                .join();

        assertEquals(1, incoming.closeCount);
        assertTrue(fixture.manager.getChildren().isEmpty());
        assertEquals(1, fixture.server.byteWrites.size());
        assertTrue(fixture.diagnostic.contains("rejected"));
    }

    @Test
    void directChildHandoffStartsWritesBranchInfoAndRaisesEvents() {
        Fixture fixture = fixture();
        fixture.manager.promoteToBranchRoot();
        ConnectionProbe incoming = ConnectionProbe.connection(ENDPOINT);
        ConnectionProbe child = ConnectionProbe.message(USERNAME, ENDPOINT);
        fixture.factory.distributedHandoff = child;
        AtomicInteger added = new AtomicInteger();
        AtomicInteger states = new AtomicInteger();
        fixture.manager.addChildAddedListener((sender, args) -> {
            assertEquals(USERNAME, args.getUsername());
            added.incrementAndGet();
        });
        fixture.manager.addStateChangedListener((sender, args) -> states.incrementAndGet());

        fixture.manager
                .addOrUpdateChildConnectionAsync(USERNAME, incoming.connection())
                .join();

        assertEquals(1, incoming.handoffCount);
        assertEquals(1, incoming.closeCount);
        assertEquals(1, child.startReadingCount);
        assertEquals(ConnectionTypes.INBOUND.or(ConnectionTypes.DIRECT), child.type);
        assertArrayEquals(fixture.manager.getBranchInformation(), child.byteWrites.getFirst());
        assertEquals(List.of(new PeerEndpoint(USERNAME, ENDPOINT)), fixture.manager.getChildren());
        assertEquals(1, added.get());
        assertTrue(states.get() >= 1);
    }

    @Test
    void directChildSupersedesDisconnectsAndClosesOldChild() {
        Fixture fixture = fixture();
        fixture.manager.promoteToBranchRoot();
        ConnectionProbe first = ConnectionProbe.message(USERNAME, ENDPOINT);
        fixture.factory.distributedHandoff = first;
        fixture.manager
                .addOrUpdateChildConnectionAsync(
                        USERNAME, ConnectionProbe.connection(ENDPOINT).connection())
                .join();

        ConnectionProbe second = ConnectionProbe.message(USERNAME, endpoint(42002));
        fixture.factory.distributedHandoff = second;
        fixture.manager
                .addOrUpdateChildConnectionAsync(
                        USERNAME, ConnectionProbe.connection(endpoint(42002)).connection())
                .join();

        assertEquals(1, first.disconnectCount);
        assertEquals("Superseded.", first.lastDisconnectMessage);
        assertEquals(1, first.closeCount);
        assertEquals(1, fixture.manager.getChildren().size());
        assertEquals(endpoint(42002), fixture.manager.getChildren().getFirst().ipEndPoint());
        assertTrue(fixture.diagnostic.contains("Updated child"));
    }

    @Test
    void directChildNegotiationFailureClosesPurgesAndMaps() {
        Fixture fixture = fixture();
        fixture.manager.promoteToBranchRoot();
        ConnectionProbe child = ConnectionProbe.message(USERNAME, ENDPOINT);
        IllegalStateException expected = new IllegalStateException("write");
        child.writeFuture = CompletableFuture.failedFuture(expected);
        fixture.factory.distributedHandoff = child;

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> fixture.manager
                        .addOrUpdateChildConnectionAsync(
                                USERNAME, ConnectionProbe.connection(ENDPOINT).connection())
                        .join());

        ConnectionException mapped = assertInstanceOf(ConnectionException.class, thrown.getCause());
        assertSame(expected, mapped.getCause());
        assertEquals(1, child.closeCount);
        assertTrue(fixture.manager.getChildren().isEmpty());
    }

    @Test
    void indirectChildConnectsPiercesWritesBranchAndCaches() {
        Fixture fixture = fixture();
        fixture.manager.promoteToBranchRoot();
        ConnectionProbe child = ConnectionProbe.message(USERNAME, ENDPOINT);
        fixture.factory.distributedDirect.put(ENDPOINT, child);
        ConnectToPeerResponse response =
                new ConnectToPeerResponse(USERNAME, Constants.ConnectionType.DISTRIBUTED, ENDPOINT, TOKEN, false);
        AtomicInteger added = new AtomicInteger();
        fixture.manager.addChildAddedListener((sender, args) -> added.incrementAndGet());

        fixture.manager.getOrAddChildConnectionAsync(response).join();
        fixture.manager.getOrAddChildConnectionAsync(response).join();

        assertEquals(1, child.connectCount);
        assertEquals(2, child.byteWrites.size());
        assertArrayEquals(new PierceFirewall(TOKEN).toByteArray(), child.byteWrites.get(0));
        assertArrayEquals(fixture.manager.getBranchInformation(), child.byteWrites.get(1));
        assertEquals(ConnectionTypes.INBOUND.or(ConnectionTypes.INDIRECT), child.type);
        assertEquals(1, added.get());
        assertTrue(fixture.manager.getPendingSolicitations().isEmpty());
        assertTrue(fixture.diagnostic.contains("already exists"));
    }

    @Test
    void indirectChildFailureClosesPurgesAndMaps() {
        Fixture fixture = fixture();
        fixture.manager.promoteToBranchRoot();
        ConnectionProbe child = ConnectionProbe.message(USERNAME, ENDPOINT);
        IllegalStateException expected = new IllegalStateException("connect");
        child.connectFuture = CompletableFuture.failedFuture(expected);
        fixture.factory.distributedDirect.put(ENDPOINT, child);

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> fixture.manager
                        .getOrAddChildConnectionAsync(new ConnectToPeerResponse(
                                USERNAME, Constants.ConnectionType.DISTRIBUTED, ENDPOINT, TOKEN, false))
                        .join());

        ConnectionException mapped = assertInstanceOf(ConnectionException.class, thrown.getCause());
        assertSame(expected, mapped.getCause());
        assertEquals(1, child.closeCount);
        assertTrue(fixture.manager.getChildren().isEmpty());
    }

    @Test
    void childDisconnectRemovesClosesAndRaisesEvents() {
        Fixture fixture = fixture();
        fixture.manager.promoteToBranchRoot();
        ConnectionProbe child = ConnectionProbe.message(USERNAME, ENDPOINT);
        fixture.factory.distributedHandoff = child;
        fixture.manager
                .addOrUpdateChildConnectionAsync(
                        USERNAME, ConnectionProbe.connection(ENDPOINT).connection())
                .join();
        AtomicInteger disconnected = new AtomicInteger();
        fixture.manager.addChildDisconnectedListener((sender, args) -> {
            assertEquals(USERNAME, args.getUsername());
            disconnected.incrementAndGet();
        });

        child.fireDisconnected(null, null);

        assertTrue(fixture.manager.getChildren().isEmpty());
        assertEquals(1, disconnected.get());
        // The source retains its provisional disposal callback, so an
        // established child's disconnect invokes both disposal handlers.
        assertEquals(2, child.closeCount);
        assertTrue(fixture.diagnostic.contains("disconnected."));
    }

    @Test
    void broadcastWritesOnlyConnectedChildrenAndTracksLatency() {
        Fixture fixture = fixture();
        fixture.manager.promoteToBranchRoot();
        ConnectionProbe connected = ConnectionProbe.message("a", ENDPOINT);
        fixture.factory.distributedHandoff = connected;
        fixture.manager
                .addOrUpdateChildConnectionAsync(
                        "a", ConnectionProbe.connection(ENDPOINT).connection())
                .join();
        ConnectionProbe disconnected = ConnectionProbe.message("b", endpoint(42002));
        disconnected.state = ConnectionState.DISCONNECTED;
        fixture.factory.distributedHandoff = disconnected;
        fixture.manager
                .addOrUpdateChildConnectionAsync(
                        "b", ConnectionProbe.connection(endpoint(42002)).connection())
                .join();
        connected.byteWrites.clear();
        disconnected.byteWrites.clear();

        byte[] payload = {1, 2, 3};
        fixture.manager.broadcastMessageAsync(payload).join();
        Double first = fixture.manager.getAverageBroadcastLatency();
        fixture.manager.broadcastMessageAsync(payload).join();

        assertArrayEquals(payload, connected.byteWrites.getFirst());
        assertTrue(disconnected.byteWrites.isEmpty());
        assertNotNull(first);
        assertNotNull(fixture.manager.getAverageBroadcastLatency());
    }

    @Test
    void broadcastFailureDisconnectsChildWithoutFailingBroadcast() {
        Fixture fixture = fixture();
        fixture.manager.promoteToBranchRoot();
        ConnectionProbe child = ConnectionProbe.message(USERNAME, ENDPOINT);
        fixture.factory.distributedHandoff = child;
        fixture.manager
                .addOrUpdateChildConnectionAsync(
                        USERNAME, ConnectionProbe.connection(ENDPOINT).connection())
                .join();
        child.writeFuture = CompletableFuture.failedFuture(new RuntimeException("failure"));

        assertDoesNotThrow(
                () -> fixture.manager.broadcastMessageAsync(new byte[] {9}).join());
        assertEquals(1, child.disconnectCount);
        assertTrue(child.lastDisconnectMessage.contains("Broadcast failure"));
    }

    @Test
    void statusWritesExactCommandsAndDeduplicatesUnchangedState() {
        Fixture fixture = fixture();
        fixture.server.byteWrites.clear();
        fixture.manager.updateStatusAsync().join();

        byte[] expected = concatenate(
                new BranchLevelCommand(0).toByteArray(),
                new BranchRootCommand(LOCAL_USER).toByteArray(),
                new AcceptChildrenCommand(false).toByteArray(),
                new HaveNoParentsCommand(true).toByteArray());
        assertArrayEquals(expected, fixture.server.byteWrites.getFirst());
        fixture.manager.updateStatusAsync().join();
        assertEquals(1, fixture.server.byteWrites.size());
        assertTrue(fixture.diagnostic.contains("Update skipped"));
    }

    @Test
    void statusSkipsWhenNotConnectedAndReportsWriteFailures() {
        Fixture skipped = fixture();
        skipped.client.state = SoulseekClientStates.DISCONNECTED;
        skipped.manager.updateStatusAsync().join();
        assertTrue(skipped.server.byteWrites.isEmpty());

        Fixture failed = fixture();
        failed.server.writeFuture = CompletableFuture.failedFuture(new RuntimeException("server"));
        failed.manager.updateStatusAsync().join();
        assertTrue(failed.diagnostic.containsWarning("Failed to update distributed status"));
    }

    @Test
    void branchSettersAndResetPreserveDerivedValues() {
        Fixture fixture = fixture();
        fixture.manager.setParentBranchLevel(4);
        fixture.manager.setParentBranchRoot("root");
        assertEquals(0, fixture.manager.getBranchLevel());
        assertEquals(LOCAL_USER, fixture.manager.getBranchRoot());

        fixture.manager.promoteToBranchRoot();
        fixture.manager.resetStatus();
        assertFalse(fixture.manager.isBranchRoot());
    }

    @Test
    void branchInformationContainsDistributedLevelAndRootFrames() {
        Fixture fixture = fixture();
        fixture.manager.promoteToBranchRoot();
        assertArrayEquals(
                concatenate(
                        new DistributedBranchLevel(0).toByteArray(),
                        new DistributedBranchRoot(LOCAL_USER).toByteArray()),
                fixture.manager.getBranchInformation());
    }

    @Test
    void parentCandidateMessageCompletesRequiredWaits() {
        Fixture fixture = fixture();
        ConnectionProbe parent = ConnectionProbe.message(USERNAME, ENDPOINT);
        CompletableFuture<Integer> level =
                fixture.waiter.waitAsync(new WaitKey(Constants.WaitKey.BRANCH_LEVEL_MESSAGE, parent.id), Integer.class);
        CompletableFuture<String> root =
                fixture.waiter.waitAsync(new WaitKey(Constants.WaitKey.BRANCH_ROOT_MESSAGE, parent.id), String.class);
        CompletableFuture<Void> search =
                fixture.waiter.waitAsync(new WaitKey(Constants.WaitKey.SEARCH_REQUEST_MESSAGE, parent.id));

        fixture.manager.handleParentCandidateMessage(
                parent.messageConnection(), new MessageEventArgs(new DistributedBranchLevel(3).toByteArray()));
        fixture.manager.handleParentCandidateMessage(
                parent.messageConnection(), new MessageEventArgs(new DistributedBranchRoot("root").toByteArray()));
        fixture.manager.handleParentCandidateMessage(
                parent.messageConnection(),
                new MessageEventArgs(new DistributedSearchRequest("user", TOKEN, "query").toByteArray()));

        assertEquals(3, level.join());
        assertEquals("root", root.join());
        assertNull(search.join());
    }

    @Test
    void malformedParentCandidateMessageDisconnectsAndCloses() {
        Fixture fixture = fixture();
        ConnectionProbe parent = ConnectionProbe.message(USERNAME, ENDPOINT);

        fixture.manager.handleParentCandidateMessage(parent.messageConnection(), new MessageEventArgs(new byte[0]));

        assertEquals(1, parent.disconnectCount);
        assertEquals(1, parent.closeCount);
        assertTrue(fixture.diagnostic.contains("Failed to handle message from parent candidate"));
    }

    @Test
    void parentDirectConnectionInitializesAdoptsAndRaisesEvents() {
        Fixture fixture = fixture();
        ConnectionProbe parent = initializedParent(USERNAME, ENDPOINT, 2, "root");
        fixture.factory.distributedDirect.put(ENDPOINT, parent);
        AtomicInteger adopted = new AtomicInteger();
        AtomicInteger states = new AtomicInteger();
        fixture.manager.addParentAdoptedListener((sender, args) -> {
            assertEquals(USERNAME, args.getUsername());
            assertEquals(2, args.getBranchLevel());
            assertEquals("root", args.getBranchRoot());
            adopted.incrementAndGet();
        });
        fixture.manager.addStateChangedListener((sender, args) -> states.incrementAndGet());

        fixture.manager
                .addParentConnectionAsync(List.of(new PeerEndpoint(USERNAME, ENDPOINT)))
                .join();

        assertTrue(fixture.manager.hasParent());
        assertEquals(new PeerEndpoint(USERNAME, ENDPOINT), fixture.manager.getParent());
        assertEquals(3, fixture.manager.getBranchLevel());
        assertEquals("root", fixture.manager.getBranchRoot());
        assertEquals(1, adopted.get());
        assertTrue(states.get() >= 1);
        assertArrayEquals(
                new PeerInit(LOCAL_USER, Constants.ConnectionType.DISTRIBUTED, TOKEN + 1).toByteArray(),
                parent.byteWrites.getFirst());
    }

    @Test
    void parentSelectionPrefersLowestBranchAndDisposesOthers() {
        Fixture fixture = fixture();
        InetSocketAddress firstEndpoint = endpoint(42011);
        InetSocketAddress secondEndpoint = endpoint(42012);
        ConnectionProbe first = initializedParent("one", firstEndpoint, 4, "root");
        ConnectionProbe second = initializedParent("two", secondEndpoint, 1, "root");
        fixture.factory.distributedDirect.put(firstEndpoint, first);
        fixture.factory.distributedDirect.put(secondEndpoint, second);

        fixture.manager
                .addParentConnectionAsync(
                        List.of(new PeerEndpoint("one", firstEndpoint), new PeerEndpoint("two", secondEndpoint)))
                .join();

        assertEquals("two", fixture.manager.getParent().username());
        assertEquals(1, first.disconnectCount);
        assertEquals("Not selected.", first.lastDisconnectMessage);
        assertEquals(1, first.closeCount);
    }

    @Test
    void parentInitializationFailureDoesNotAdoptCandidate() {
        Fixture fixture = fixture();
        ConnectionProbe parent = ConnectionProbe.message(USERNAME, ENDPOINT);
        parent.onByteWrite = () -> {
            parent.fireMessageRead(new DistributedBranchLevel(2).toByteArray());
            fixture.waiter.fail(
                    new WaitKey(Constants.WaitKey.SEARCH_REQUEST_MESSAGE, parent.id),
                    new RuntimeException("missing search"));
        };
        fixture.factory.distributedDirect.put(ENDPOINT, parent);

        fixture.manager
                .addParentConnectionAsync(List.of(new PeerEndpoint(USERNAME, ENDPOINT)))
                .join();

        assertFalse(fixture.manager.hasParent());
        assertTrue(parent.disconnectCount >= 1);
        assertTrue(parent.closeCount >= 1);
        assertTrue(fixture.diagnostic.containsWarning("Failed to connect to any"));
    }

    @Test
    void indirectParentSolicitationHandoffsAndStartsReading() {
        Fixture fixture = fixture();
        ConnectionProbe direct = ConnectionProbe.message(USERNAME, ENDPOINT);
        direct.connectFuture = CompletableFuture.failedFuture(new RuntimeException("direct"));
        fixture.factory.distributedDirect.put(ENDPOINT, direct);
        ConnectionProbe accepted = ConnectionProbe.connection(endpoint(42022));
        ConnectionProbe indirect = initializedParent(USERNAME, endpoint(42022), 0, USERNAME);
        indirect.onStartReading = () -> {
            indirect.fireMessageRead(new DistributedBranchLevel(0).toByteArray());
            indirect.fireMessageRead(new DistributedSearchRequest("user", TOKEN, "query").toByteArray());
        };
        fixture.factory.distributedHandoff = indirect;
        fixture.waiter.solicitationFuture = CompletableFuture.completedFuture(accepted.connection());

        fixture.manager
                .addParentConnectionAsync(List.of(new PeerEndpoint(USERNAME, ENDPOINT)))
                .join();

        assertEquals(new PeerEndpoint(USERNAME, endpoint(42022)), fixture.manager.getParent());
        assertEquals(1, indirect.startReadingCount);
        assertEquals(1, accepted.handoffCount);
        assertTrue(fixture.manager.getPendingSolicitations().isEmpty());
        assertEquals(
                Constants.ConnectionType.DISTRIBUTED,
                assertInstanceOf(ConnectToPeerRequest.class, fixture.server.outgoingWrites.getFirst())
                        .getType());
    }

    @Test
    void parentDisconnectClearsStateClosesAndRaisesEvent() {
        Fixture fixture = fixture();
        ConnectionProbe parent = initializedParent(USERNAME, ENDPOINT, 0, USERNAME);
        fixture.factory.distributedDirect.put(ENDPOINT, parent);
        fixture.manager
                .addParentConnectionAsync(List.of(new PeerEndpoint(USERNAME, ENDPOINT)))
                .join();
        AtomicInteger disconnected = new AtomicInteger();
        fixture.manager.addParentDisconnectedListener((sender, args) -> {
            assertEquals(USERNAME, args.getUsername());
            disconnected.incrementAndGet();
        });
        // Avoid reconnecting to the same test candidate.
        fixture.client.state = SoulseekClientStates.DISCONNECTED;

        parent.fireDisconnected("gone", null);

        assertFalse(fixture.manager.hasParent());
        assertEquals(1, disconnected.get());
        assertEquals(1, parent.closeCount);
        assertTrue(fixture.diagnostic.contains("Parent connection"));
    }

    @Test
    void watchdogRequestsStatusOnlyWhenEligible() {
        Fixture fixture = fixture();
        fixture.server.byteWrites.clear();
        fixture.manager.watchdogElapsed();
        assertTrue(fixture.diagnostic.containsWarning("No distributed parent connected"));
        assertEquals(1, fixture.server.byteWrites.size());

        Fixture disconnected = fixture();
        disconnected.client.state = SoulseekClientStates.DISCONNECTED;
        disconnected.manager.watchdogElapsed();
        assertFalse(disconnected.diagnostic.containsWarning("No distributed parent connected"));
    }

    @Test
    void removeAllClearsParentChildrenAndSolicitations() {
        Fixture fixture = fixture();
        fixture.manager.promoteToBranchRoot();
        ConnectionProbe child = ConnectionProbe.message(USERNAME, ENDPOINT);
        fixture.factory.distributedHandoff = child;
        fixture.manager
                .addOrUpdateChildConnectionAsync(
                        USERNAME, ConnectionProbe.connection(ENDPOINT).connection())
                .join();

        fixture.manager.removeAndDisposeAll();

        assertTrue(fixture.manager.getChildren().isEmpty());
        assertTrue(fixture.manager.getPendingSolicitations().isEmpty());
        assertEquals(1, child.closeCount);
    }

    private Fixture fixture() {
        Fixture fixture = new Fixture();
        managers.add(fixture.manager);
        return fixture;
    }

    private static ConnectionProbe initializedParent(
            String username, InetSocketAddress endpoint, int level, String root) {
        ConnectionProbe parent = ConnectionProbe.message(username, endpoint);
        parent.onByteWrite = () -> {
            parent.fireMessageRead(new DistributedBranchLevel(level).toByteArray());
            if (level > 0) {
                parent.fireMessageRead(new DistributedBranchRoot(root).toByteArray());
            }
            parent.fireMessageRead(new DistributedSearchRequest("user", TOKEN, "query").toByteArray());
        };
        return parent;
    }

    private static InetSocketAddress endpoint(int port) {
        try {
            return new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port);
        } catch (Exception exception) {
            throw new AssertionError(exception);
        }
    }

    private static byte[] concatenate(byte[]... arrays) {
        int length = Arrays.stream(arrays).mapToInt(array -> array.length).sum();
        byte[] result = new byte[length];
        int offset = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
    }

    private static final class Fixture {
        private final RecordingDiagnostic diagnostic = new RecordingDiagnostic();
        private final FakeWaiter waiter = new FakeWaiter();
        private final ConnectionProbe server = ConnectionProbe.message("", endpoint(2242));
        private final FakeFactory factory = new FakeFactory();
        private final FakeClient client = new FakeClient(waiter, server.messageConnection());
        private final DefaultDistributedConnectionManager manager =
                new DefaultDistributedConnectionManager(client, factory, diagnostic);
    }

    private static final class FakeClient implements DistributedConnectionManagerClient {
        private final FakeWaiter waiter;
        private final MessageConnection server;
        private final AtomicInteger token = new AtomicInteger(TOKEN);
        private final DistributedMessageHandler handler = (DistributedMessageHandler) Proxy.newProxyInstance(
                DistributedMessageHandler.class.getClassLoader(),
                new Class<?>[] {DistributedMessageHandler.class},
                (proxy, method, arguments) -> defaultValue(method.getReturnType()));
        private SoulseekClientStates state = SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGED_IN);

        private FakeClient(FakeWaiter waiter, MessageConnection server) {
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
        public SoulseekClientStates getState() {
            return state;
        }

        @Override
        public int getNextToken() {
            return token.getAndIncrement();
        }

        @Override
        public Waiter getWaiter() {
            return waiter;
        }

        @Override
        public MessageConnection getServerConnection() {
            return server;
        }

        @Override
        public DistributedMessageHandler getDistributedMessageHandler() {
            return handler;
        }
    }

    private static final class FakeFactory implements ConnectionFactory {
        private final Map<InetSocketAddress, ConnectionProbe> distributedDirect = new HashMap<>();
        private ConnectionProbe distributedHandoff;

        @Override
        public MessageConnection getDistributedConnection(
                String username, InetSocketAddress ipEndPoint, ConnectionOptions options, TcpClient tcpClient) {
            if (tcpClient != null) {
                assertNotNull(distributedHandoff);
                return distributedHandoff.messageConnection();
            }
            ConnectionProbe connection = distributedDirect.get(ipEndPoint);
            assertNotNull(connection, "No distributed connection for endpoint");
            return connection.messageConnection();
        }

        @Override
        public MessageConnection getMessageConnection(
                String username, InetSocketAddress ipEndPoint, ConnectionOptions options, TcpClient tcpClient) {
            throw new AssertionError("unexpected peer connection");
        }

        @Override
        public MessageConnection getServerConnection(
                InetSocketAddress ipEndPoint,
                ConnectionEventListener<Void> connectedEventHandler,
                ConnectionEventListener<ConnectionDisconnectedEventArgs> disconnectedEventHandler,
                MessageConnectionEventListener<MessageEventArgs> messageReadEventHandler,
                MessageConnectionEventListener<MessageEventArgs> messageWrittenEventHandler,
                ConnectionOptions options,
                TcpClient tcpClient) {
            throw new AssertionError("unexpected server connection");
        }

        @Override
        public Connection getTransferConnection(
                InetSocketAddress ipEndPoint, ConnectionOptions options, TcpClient tcpClient) {
            throw new AssertionError("unexpected transfer connection");
        }
    }

    private static final class FakeWaiter implements Waiter {
        private final Map<WaitKey, CompletableFuture<?>> futures = new HashMap<>();
        private CompletableFuture<?> solicitationFuture =
                CompletableFuture.failedFuture(new IllegalStateException("No solicitation configured"));

        @Override
        public int getDefaultTimeout() {
            return 5_000;
        }

        @Override
        public void cancel(WaitKey key) {
            CompletableFuture<?> future = futures.remove(key);
            if (future != null) {
                future.cancel(false);
            }
        }

        @Override
        public void cancelAll() {
            futures.values().forEach(future -> future.cancel(false));
            futures.clear();
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
            if (key.getToken().startsWith(Constants.WaitKey.SOLICITED_DISTRIBUTED_CONNECTION)) {
                return (CompletableFuture<T>) solicitationFuture;
            }
            return (CompletableFuture<T>) futures.computeIfAbsent(key, ignored -> new CompletableFuture<>());
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
        private final List<String> warnings = new ArrayList<>();

        private boolean contains(String value) {
            return messages.stream().anyMatch(message -> message.toLowerCase().contains(value.toLowerCase()));
        }

        private boolean containsWarning(String value) {
            return warnings.stream().anyMatch(message -> message.toLowerCase().contains(value.toLowerCase()));
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
            warnings.add(message);
        }

        @Override
        public void warning(String message, Throwable exception) {
            messages.add(message);
            warnings.add(message);
        }
    }

    private static final class ConnectionProbe implements InvocationHandler {
        private final boolean message;
        private final String username;
        private final InetSocketAddress endpoint;
        private final UUID id = UUID.randomUUID();
        private final TcpClient tcpClient = (TcpClient) Proxy.newProxyInstance(
                TcpClient.class.getClassLoader(),
                new Class<?>[] {TcpClient.class},
                (proxy, method, arguments) -> defaultValue(method.getReturnType()));
        private final Connection proxy;
        private final List<ConnectionEventListener<ConnectionDisconnectedEventArgs>> disconnectedListeners =
                new ArrayList<>();
        private final List<MessageConnectionEventListener<MessageEventArgs>> messageReadListeners = new ArrayList<>();
        private final List<byte[]> byteWrites = new ArrayList<>();
        private final List<OutgoingMessage> outgoingWrites = new ArrayList<>();
        private ConnectionTypes type = ConnectionTypes.NONE;
        private ConnectionState state = ConnectionState.CONNECTED;
        private CompletableFuture<Void> connectFuture = CompletableFuture.completedFuture(null);
        private CompletableFuture<Void> writeFuture = CompletableFuture.completedFuture(null);
        private Runnable onByteWrite;
        private Runnable onStartReading;
        private int closeCount;
        private int connectCount;
        private int disconnectCount;
        private int handoffCount;
        private int startReadingCount;
        private String lastDisconnectMessage;

        private ConnectionProbe(boolean message, String username, InetSocketAddress endpoint) {
            this.message = message;
            this.username = username;
            this.endpoint = endpoint;
            proxy = (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    message ? new Class<?>[] {MessageConnection.class} : new Class<?>[] {Connection.class},
                    this);
        }

        private static ConnectionProbe connection(InetSocketAddress endpoint) {
            return new ConnectionProbe(false, null, endpoint);
        }

        private static ConnectionProbe message(String username, InetSocketAddress endpoint) {
            return new ConnectionProbe(true, username, endpoint);
        }

        private Connection connection() {
            return proxy;
        }

        private MessageConnection messageConnection() {
            return (MessageConnection) proxy;
        }

        private void fireDisconnected(String text, Exception exception) {
            ConnectionDisconnectedEventArgs eventArgs = new ConnectionDisconnectedEventArgs(text, exception);
            List.copyOf(disconnectedListeners).forEach(listener -> listener.handle(proxy, eventArgs));
        }

        private void fireMessageRead(byte[] bytes) {
            MessageEventArgs eventArgs = new MessageEventArgs(bytes);
            List.copyOf(messageReadListeners).forEach(listener -> listener.handle(messageConnection(), eventArgs));
        }

        @Override
        public Object invoke(Object ignored, Method method, Object[] arguments) {
            return switch (method.getName()) {
                case "getId" -> id;
                case "getInactiveTime" -> Duration.ZERO;
                case "getIpEndPoint" -> endpoint;
                case "getKey" -> new ConnectionKey(username, endpoint);
                case "getOptions" -> new ConnectionOptions();
                case "getState" -> state;
                case "getType" -> type;
                case "setType" -> {
                    type = (ConnectionTypes) arguments[0];
                    yield null;
                }
                case "getWriteQueueDepth" -> 0;
                case "getCodeLength" -> 1;
                case "isServerConnection" -> username == null || username.isEmpty();
                case "isReadingContinuously" -> startReadingCount > 0;
                case "getUsername" -> username == null ? "" : username;
                case "connectAsync" -> {
                    connectCount++;
                    yield connectFuture;
                }
                case "writeAsync" -> {
                    if (arguments[0] instanceof byte[] bytes) {
                        byteWrites.add(Arrays.copyOf(bytes, bytes.length));
                        if (onByteWrite != null) {
                            onByteWrite.run();
                        }
                    } else if (arguments[0] instanceof OutgoingMessage value) {
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
                    if (onStartReading != null) {
                        onStartReading.run();
                    }
                    yield null;
                }
                case "close" -> {
                    closeCount++;
                    yield null;
                }
                case "disconnect" -> {
                    disconnectCount++;
                    lastDisconnectMessage = arguments == null || arguments.length == 0 ? null : (String) arguments[0];
                    yield null;
                }
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
