// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.common.TokenBucket;
import dev.slsk.common.TokenFactory;
import dev.slsk.common.Waiter;
import dev.slsk.diagnostics.DiagnosticEvent;
import dev.slsk.diagnostics.DiagnosticEventListener;
import dev.slsk.events.DistributedChildEvent;
import dev.slsk.events.DownloadDeniedEvent;
import dev.slsk.events.DownloadFailedEvent;
import dev.slsk.events.SoulseekClientDisconnectedEvent;
import dev.slsk.exceptions.KickedFromServerException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferReportedFailedException;
import dev.slsk.messaging.handlers.DistributedMessageHandler;
import dev.slsk.messaging.handlers.PeerMessageHandler;
import dev.slsk.messaging.handlers.PeerMessageHandlerEventListener;
import dev.slsk.messaging.handlers.ServerMessageEvent;
import dev.slsk.messaging.handlers.ServerMessageHandler;
import dev.slsk.messaging.handlers.ServerMessageHandlerEventListener;
import dev.slsk.network.ConnectionFactory;
import dev.slsk.network.DistributedConnectionManager;
import dev.slsk.network.ListenerHandler;
import dev.slsk.network.MessageConnection;
import dev.slsk.network.PeerConnectionManager;
import dev.slsk.network.PeerEndpoint;
import dev.slsk.options.SoulseekClientOptions;
import dev.slsk.search.SearchInternal;
import dev.slsk.search.SearchResponder;
import dev.slsk.transfer.TransferInternal;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class SoulseekClientTest {
    private static final InetSocketAddress ENDPOINT = endpoint(46001);

    @Test
    void constructorsPreserveVersionOptionsAndInitialState() {
        assertThrows(IllegalArgumentException.class, () -> new DefaultSoulseekClient(100));
        SoulseekClientOptions options = new SoulseekClientOptions();
        try (DefaultSoulseekClient client = new DefaultSoulseekClient(9999, options)) {
            assertEquals(170, client.getMajorVersion());
            assertEquals(9999, client.getMinorVersion());
            assertSame(options, client.getOptions());
            assertEquals(SoulseekClientStates.DISCONNECTED, client.getState());
            assertNull(client.getUsername());
            assertNull(client.getIpEndpoint());
            assertNull(client.getIpAddress());
            assertNull(client.getPort());
            assertNull(client.getServerInfo().getParentMinSpeed());
        }
    }

    @Test
    void endpointAndTransferPropertiesReturnSnapshots() {
        Fixture fixture = new Fixture();
        fixture.client.setIpEndpointForTest(ENDPOINT);
        Map<Integer, TransferInternal> downloads = new HashMap<>();
        Map<Integer, TransferInternal> uploads = new HashMap<>();
        downloads.put(1, new TransferInternal(TransferDirection.DOWNLOAD, "d", "download", 1));
        uploads.put(2, new TransferInternal(TransferDirection.UPLOAD, "u", "upload", 2));
        fixture.client.setDownloadsForTest(downloads);
        fixture.client.setUploadsForTest(uploads);
        List<Transfer> downloadSnapshot = fixture.client.getDownloads();
        List<Transfer> uploadSnapshot = fixture.client.getUploads();

        assertEquals(ENDPOINT, fixture.client.getIpEndpoint());
        assertEquals(ENDPOINT.getAddress(), fixture.client.getIpAddress());
        assertEquals(ENDPOINT.getPort(), fixture.client.getPort());
        assertEquals("download", fixture.client.getDownloads().getFirst().getFilename());
        assertEquals("upload", fixture.client.getUploads().getFirst().getFilename());

        downloads.clear();
        uploads.clear();
        assertEquals(1, downloadSnapshot.size());
        assertEquals(1, uploadSnapshot.size());
        fixture.close();
    }

    @Test
    void stateChangesRaiseSourceEventsSynchronously() {
        Fixture fixture = new Fixture();
        List<String> order = new ArrayList<>();
        fixture.client.addStateChangedListener((sender, eventData) -> order.add("state:" + eventData.getState()));
        fixture.client.addConnectedListener((sender, eventData) -> order.add("connected"));
        fixture.client.addLoggedInListener((sender, eventData) -> order.add("logged"));
        AtomicReference<SoulseekClientDisconnectedEvent> disconnected = new AtomicReference<>();
        fixture.client.addDisconnectedListener((sender, eventData) -> disconnected.set(eventData));

        fixture.client.changeState(SoulseekClientStates.CONNECTED, "connected", null);
        fixture.client.changeState(SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGED_IN), "logged", null);
        RuntimeException cause = new RuntimeException("bye");
        fixture.client.changeState(SoulseekClientStates.DISCONNECTED, "bye", cause);

        assertEquals(
                List.of("state:CONNECTED", "connected", "state:CONNECTED | LOGGED_IN", "logged", "state:DISCONNECTED"),
                order);
        assertEquals("bye", disconnected.get().getMessage());
        assertSame(cause, disconnected.get().getException());
        fixture.close();
    }

    @Test
    void disconnectUsesSourceReasonsCancelsSearchesAndRetainsDownloads() {
        Fixture fixture = new Fixture();
        fixture.client.setStateForTest(SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGED_IN));
        SearchInternal search = new SearchInternal(SearchQuery.fromText("query"), SearchScope.getNetwork(), 1);
        Map<Integer, SearchInternal> searches = new HashMap<>();
        searches.put(1, search);
        fixture.client.setSearchesForTest(searches);
        Map<Integer, TransferInternal> downloads = new HashMap<>();
        downloads.put(1, new TransferInternal(TransferDirection.DOWNLOAD, "user", "file", 1));
        fixture.client.setDownloadsForTest(downloads);

        fixture.client.disconnect(null, new RuntimeException("cause"));

        assertEquals(SoulseekClientStates.DISCONNECTED, fixture.client.getState());
        assertEquals("cause", fixture.connection.disconnectMessage);
        assertTrue(searches.isEmpty());
        assertEquals(1, downloads.size());
        assertEquals(1, fixture.distributed.removed);
        assertEquals(1, fixture.distributed.resets);

        fixture.client.disconnect();
        assertEquals(1, fixture.connection.disconnects);
        fixture.close();
    }

    @Test
    void deniedAndFailedDownloadsCompleteAllMatchesAndAlwaysRaise() {
        Fixture fixture = new Fixture();
        TransferInternal first = new TransferInternal(TransferDirection.DOWNLOAD, "user", "file", 1);
        TransferInternal second = new TransferInternal(TransferDirection.DOWNLOAD, "user", "file", 2);
        fixture.client.setDownloadsForTest(new HashMap<>(Map.of(
                1, first,
                2, second)));
        AtomicReference<DownloadDeniedEvent> denied = new AtomicReference<>();
        AtomicReference<DownloadFailedEvent> failed = new AtomicReference<>();
        fixture.client.addDownloadDeniedListener((sender, eventData) -> denied.set(eventData));
        fixture.client.addDownloadFailedListener((sender, eventData) -> failed.set(eventData));

        fixture.peer.raiseDenied(new DownloadDeniedEvent("user", "file", "rejected"));
        assertInstanceOf(TransferRejectedException.class, failure(first.getRemoteTaskCompletionSource()));
        assertInstanceOf(TransferRejectedException.class, failure(second.getRemoteTaskCompletionSource()));
        assertEquals("rejected", denied.get().getMessage());

        first = new TransferInternal(TransferDirection.DOWNLOAD, "user", "file", 3);
        second = new TransferInternal(TransferDirection.DOWNLOAD, "user", "file", 4);
        fixture.client.setDownloadsForTest(new HashMap<>(Map.of(
                3, first,
                4, second)));
        fixture.peer.raiseFailed(new DownloadFailedEvent("user", "file"));
        assertInstanceOf(TransferReportedFailedException.class, failure(first.getRemoteTaskCompletionSource()));
        assertInstanceOf(TransferReportedFailedException.class, failure(second.getRemoteTaskCompletionSource()));
        assertEquals("file", failed.get().getFilename());
        fixture.close();
    }

    @Test
    void serverEventsForwardUpdateInfoAndKickDisconnects() {
        Fixture fixture = new Fixture();
        AtomicReference<String> global = new AtomicReference<>();
        AtomicReference<ServerInfo> serverInfo = new AtomicReference<>();
        AtomicInteger kicked = new AtomicInteger();
        fixture.client.addGlobalMessageReceivedListener((sender, value) -> global.set(value));
        fixture.client.addServerInfoReceivedListener((sender, value) -> serverInfo.set(value));
        fixture.client.addKickedFromServerListener((sender, value) -> kicked.incrementAndGet());

        fixture.server.raise(ServerMessageEvent.GLOBAL_MESSAGE_RECEIVED, "global");
        fixture.server.raise(ServerMessageEvent.SERVER_INFO_RECEIVED, new ServerInfo(1, 2, 3, true));
        fixture.client.setStateForTest(SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGED_IN));
        fixture.server.raise(ServerMessageEvent.KICKED_FROM_SERVER, null);

        assertEquals("global", global.get());
        assertEquals(2, serverInfo.get().getParentSpeedRatio());
        assertEquals(true, fixture.client.getServerInfo().isSupporter());
        assertEquals(1, kicked.get());
        assertEquals(SoulseekClientStates.DISCONNECTED, fixture.client.getState());
        AtomicReference<SoulseekClientDisconnectedEvent> disconnect = new AtomicReference<>();
        fixture.client.addDisconnectedListener((sender, value) -> disconnect.set(value));
        fixture.client.setStateForTest(SoulseekClientStates.CONNECTED);
        fixture.server.raise(ServerMessageEvent.KICKED_FROM_SERVER, null);
        assertInstanceOf(KickedFromServerException.class, disconnect.get().getException());
        fixture.close();
    }

    @Test
    void subsystemEventsForwardSenderPayloadAndListenerRemoval() {
        Fixture fixture = new Fixture();
        AtomicReference<Object> diagnosticSender = new AtomicReference<>();
        AtomicReference<DiagnosticEvent> diagnostic = new AtomicReference<>();
        DiagnosticEventListener diagnosticListener = (sender, value) -> {
            diagnosticSender.set(sender);
            diagnostic.set(value);
        };
        fixture.client.addDiagnosticGeneratedListener(diagnosticListener);
        DiagnosticEvent expected = new DiagnosticEvent(dev.slsk.diagnostics.DiagnosticLevel.INFO, "message");
        fixture.search.raiseDiagnostic(expected);
        assertSame(fixture.search.proxy, diagnosticSender.get());
        assertSame(expected, diagnostic.get());
        fixture.client.removeDiagnosticGeneratedListener(diagnosticListener);

        AtomicReference<DistributedChildEvent> child = new AtomicReference<>();
        fixture.client.addDistributedChildAddedListener((sender, value) -> child.set(value));
        DistributedChildEvent childArgs = new DistributedChildEvent("child", ENDPOINT);
        fixture.distributed.raise("addChildAddedListener", childArgs);
        assertSame(childArgs, child.get());
        fixture.close();
    }

    @Test
    void distributedNetworkReflectsManagerSnapshot() {
        Fixture fixture = new Fixture();
        fixture.distributed.branchLevel = 2;
        fixture.distributed.branchRoot = "root";
        fixture.distributed.branchRootNode = true;
        fixture.distributed.childLimit = 3;
        fixture.distributed.canAccept = true;
        fixture.distributed.parent = new PeerEndpoint("parent", ENDPOINT);
        fixture.distributed.children = List.of(new PeerEndpoint("child", ENDPOINT));
        fixture.distributed.averageLatency = 12.5;

        DistributedNetworkInfo info = fixture.client.getDistributedNetwork();

        assertEquals(2, info.getBranchLevel());
        assertEquals("root", info.getBranchRoot());
        assertTrue(info.isBranchRoot());
        assertEquals("parent", info.getParent().username());
        assertEquals("child", info.getChildren().getFirst().username());
        assertEquals(12.5, info.getAverageBroadcastLatency());
        fixture.close();
    }

    @Test
    void everyPublicSourceEventHasAddAndRemoveMethods() {
        String[] names = {
            "BrowseProgressUpdated",
            "Connected",
            "DemotedFromDistributedBranchRoot",
            "DiagnosticGenerated",
            "Disconnected",
            "DistributedChildAdded",
            "DistributedChildDisconnected",
            "DistributedNetworkReset",
            "DistributedNetworkStateChanged",
            "DistributedParentAdopted",
            "DistributedParentDisconnected",
            "DownloadDenied",
            "DownloadFailed",
            "ExcludedSearchPhrasesReceived",
            "GlobalMessageReceived",
            "KickedFromServer",
            "LoggedIn",
            "PrivateMessageReceived",
            "PrivateRoomMembershipAdded",
            "PrivateRoomMembershipRemoved",
            "PrivateRoomModeratedUserListReceived",
            "PrivateRoomModerationAdded",
            "PrivateRoomModerationRemoved",
            "PrivateRoomUserListReceived",
            "PrivilegedUserListReceived",
            "PrivilegeNotificationReceived",
            "PromotedToDistributedBranchRoot",
            "PublicChatMessageReceived",
            "RoomJoined",
            "RoomLeft",
            "RoomListReceived",
            "RoomMessageReceived",
            "RoomTickerAdded",
            "RoomTickerListReceived",
            "RoomTickerRemoved",
            "SearchRequestReceived",
            "SearchResponseDelivered",
            "SearchResponseDeliveryFailed",
            "SearchResponseReceived",
            "SearchStateChanged",
            "ServerInfoReceived",
            "StateChanged",
            "TransferProgressUpdated",
            "TransferStateChanged",
            "UserCannotConnect",
            "UserStatisticsChanged",
            "UserStatusChanged"
        };
        for (String name : names) {
            assertTrue(hasMethod("add" + name + "Listener"));
            assertTrue(hasMethod("remove" + name + "Listener"));
        }
    }

    private static boolean hasMethod(String name) {
        for (Method method : DefaultSoulseekClient.class.getMethods()) {
            if (method.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static Throwable failure(CompletableFuture<?> future) {
        try {
            future.join();
            throw new AssertionError("future did not fail");
        } catch (java.util.concurrent.CompletionException exception) {
            return exception.getCause();
        }
    }

    private static InetSocketAddress endpoint(int port) {
        try {
            return new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port);
        } catch (Exception exception) {
            throw new AssertionError(exception);
        }
    }

    private static Object defaultValue(Class<?> type) {
        if (type == CompletableFuture.class) {
            return CompletableFuture.completedFuture(null);
        }
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
            return 0d;
        }
        return null;
    }

    private static final class Fixture implements AutoCloseable {
        private final ConnectionProbe connection = new ConnectionProbe();
        private final PeerHandlerProbe peer = new PeerHandlerProbe();
        private final ServerHandlerProbe server = new ServerHandlerProbe();
        private final SearchResponderProbe search = new SearchResponderProbe();
        private final DistributedManagerProbe distributed = new DistributedManagerProbe();
        private final ListenerHandler listenerHandler = diagnosticProxy(ListenerHandler.class);
        private final DistributedMessageHandler distributedHandler = diagnosticProxy(DistributedMessageHandler.class);
        private final PeerConnectionManager peerManager = diagnosticProxy(PeerConnectionManager.class);
        private final DefaultSoulseekClient client;

        private Fixture() {
            client = new DefaultSoulseekClient(
                    9999,
                    new SoulseekClientOptions(),
                    connection.proxy,
                    diagnosticProxy(ConnectionFactory.class),
                    peerManager,
                    distributed.proxy,
                    server.proxy,
                    peer.proxy,
                    distributedHandler,
                    null,
                    listenerHandler,
                    search.proxy,
                    new RecordingWaiter(),
                    new TokenFactory(),
                    null,
                    null,
                    new TokenBucket(1, 100),
                    new TokenBucket(1, 100));
        }

        @Override
        public void close() {
            client.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T diagnosticProxy(Class<T> type) {
        return (T) Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (ignored, method, arguments) -> defaultValue(method.getReturnType()));
    }

    private static final class ConnectionProbe {
        private String disconnectMessage;
        private int disconnects;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("disconnect")) {
                disconnects++;
                disconnectMessage = (String) arguments[0];
                return null;
            }
            if (method.getName().equals("writeAsync")) {
                return CompletableFuture.completedFuture(null);
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class PeerHandlerProbe {
        private PeerMessageHandlerEventListener<DownloadDeniedEvent> denied;
        private PeerMessageHandlerEventListener<DownloadFailedEvent> failed;
        private final PeerMessageHandler proxy = (PeerMessageHandler) Proxy.newProxyInstance(
                PeerMessageHandler.class.getClassLoader(), new Class<?>[] {PeerMessageHandler.class}, this::invoke);

        @SuppressWarnings("unchecked")
        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("addDownloadDeniedListener")) {
                denied = (PeerMessageHandlerEventListener<DownloadDeniedEvent>) arguments[0];
            } else if (method.getName().equals("addDownloadFailedListener")) {
                failed = (PeerMessageHandlerEventListener<DownloadFailedEvent>) arguments[0];
            }
            return defaultValue(method.getReturnType());
        }

        private void raiseDenied(DownloadDeniedEvent eventData) {
            denied.handle(proxy, eventData);
        }

        private void raiseFailed(DownloadFailedEvent eventData) {
            failed.handle(proxy, eventData);
        }
    }

    private static final class ServerHandlerProbe {
        private final Map<ServerMessageEvent, ServerMessageHandlerEventListener<?>> listeners = new HashMap<>();
        private final ServerMessageHandler proxy = (ServerMessageHandler) Proxy.newProxyInstance(
                ServerMessageHandler.class.getClassLoader(), new Class<?>[] {ServerMessageHandler.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("addListener")) {
                listeners.put((ServerMessageEvent) arguments[0], (ServerMessageHandlerEventListener<?>) arguments[1]);
            }
            return defaultValue(method.getReturnType());
        }

        @SuppressWarnings("unchecked")
        private <T> void raise(ServerMessageEvent event, T eventData) {
            ((ServerMessageHandlerEventListener<T>) listeners.get(event)).handle(proxy, eventData);
        }
    }

    private static final class SearchResponderProbe {
        private DiagnosticEventListener diagnostic;
        private final SearchResponder proxy = (SearchResponder) Proxy.newProxyInstance(
                SearchResponder.class.getClassLoader(), new Class<?>[] {SearchResponder.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("addDiagnosticGeneratedListener")) {
                diagnostic = (DiagnosticEventListener) arguments[0];
            }
            return defaultValue(method.getReturnType());
        }

        private void raiseDiagnostic(DiagnosticEvent eventData) {
            diagnostic.handle(proxy, eventData);
        }
    }

    private static final class DistributedManagerProbe {
        private final Map<String, Object> eventListeners = new HashMap<>();
        private int branchLevel;
        private String branchRoot = "";
        private boolean branchRootNode;
        private int childLimit;
        private boolean canAccept;
        private PeerEndpoint parent = new PeerEndpoint("", null);
        private List<PeerEndpoint> children = List.of();
        private Double averageLatency;
        private int removed;
        private int resets;
        private final DistributedConnectionManager proxy = (DistributedConnectionManager) Proxy.newProxyInstance(
                DistributedConnectionManager.class.getClassLoader(),
                new Class<?>[] {DistributedConnectionManager.class},
                this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().startsWith("add") && method.getName().endsWith("Listener")) {
                eventListeners.put(method.getName(), arguments[0]);
                return null;
            }
            return switch (method.getName()) {
                case "getBranchLevel" -> branchLevel;
                case "getBranchRoot" -> branchRoot;
                case "isBranchRoot" -> branchRootNode;
                case "getChildLimit" -> childLimit;
                case "canAcceptChildren" -> canAccept;
                case "getParent" -> parent;
                case "hasParent" -> parent != null && parent.ipEndpoint() != null;
                case "getChildren" -> children;
                case "getAverageBroadcastLatency" -> averageLatency;
                case "removeAndDisposeAll" -> {
                    removed++;
                    yield null;
                }
                case "resetStatus" -> {
                    resets++;
                    yield null;
                }
                default -> defaultValue(method.getReturnType());
            };
        }

        @SuppressWarnings("unchecked")
        private <T> void raise(String registrationMethod, T eventData) {
            dev.slsk.network.DistributedManagerEventListener<T> listener =
                    (dev.slsk.network.DistributedManagerEventListener<T>) eventListeners.get(registrationMethod);
            listener.handle(proxy, eventData);
        }
    }

    private static final class RecordingWaiter implements Waiter {
        @Override
        public int getDefaultTimeout() {
            return 5_000;
        }

        @Override
        public void cancel(dev.slsk.common.WaitKey key) {}

        @Override
        public void cancelAll() {}

        @Override
        public void complete(dev.slsk.common.WaitKey key) {}

        @Override
        public <T> void complete(dev.slsk.common.WaitKey key, T result) {}

        @Override
        public boolean hasWait(dev.slsk.common.WaitKey key) {
            return false;
        }

        @Override
        public void fail(dev.slsk.common.WaitKey key, Throwable exception) {}

        @Override
        public void timeout(dev.slsk.common.WaitKey key) {}

        @Override
        public CompletableFuture<Void> waitAsync(dev.slsk.common.WaitKey key) {
            return new CompletableFuture<>();
        }

        @Override
        public CompletableFuture<Void> waitAsync(dev.slsk.common.WaitKey key, Integer timeout) {
            return waitAsync(key);
        }

        @Override
        public CompletableFuture<Void> waitAsync(
                dev.slsk.common.WaitKey key, Integer timeout, dev.slsk.CancellationToken cancellationToken) {
            return waitAsync(key);
        }

        @Override
        public <T> CompletableFuture<T> waitAsync(dev.slsk.common.WaitKey key, Class<T> resultType) {
            return new CompletableFuture<>();
        }

        @Override
        public <T> CompletableFuture<T> waitAsync(dev.slsk.common.WaitKey key, Class<T> resultType, Integer timeout) {
            return waitAsync(key, resultType);
        }

        @Override
        public <T> CompletableFuture<T> waitAsync(
                dev.slsk.common.WaitKey key,
                Class<T> resultType,
                Integer timeout,
                dev.slsk.CancellationToken cancellationToken) {
            return waitAsync(key, resultType);
        }

        @Override
        public CompletableFuture<Void> waitIndefinitelyAsync(dev.slsk.common.WaitKey key) {
            return waitAsync(key);
        }

        @Override
        public CompletableFuture<Void> waitIndefinitelyAsync(
                dev.slsk.common.WaitKey key, dev.slsk.CancellationToken cancellationToken) {
            return waitAsync(key);
        }

        @Override
        public <T> CompletableFuture<T> waitIndefinitelyAsync(dev.slsk.common.WaitKey key, Class<T> resultType) {
            return waitAsync(key, resultType);
        }

        @Override
        public <T> CompletableFuture<T> waitIndefinitelyAsync(
                dev.slsk.common.WaitKey key, Class<T> resultType, dev.slsk.CancellationToken cancellationToken) {
            return waitAsync(key, resultType);
        }

        @Override
        public void close() {}
    }
}
