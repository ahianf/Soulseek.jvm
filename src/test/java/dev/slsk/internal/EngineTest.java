// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.Soulseek;
import dev.slsk.Subscription;
import dev.slsk.diagnostics.MeshState;
import dev.slsk.exceptions.KickedFromServerException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferReportedFailedException;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.common.TokenBucket;
import dev.slsk.internal.common.TokenFactory;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.connection.ServerSessionInfo;
import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.diagnostics.DiagnosticMessage;
import dev.slsk.internal.events.DistributedChildEvent;
import dev.slsk.internal.events.DownloadDeniedEvent;
import dev.slsk.internal.events.DownloadFailedEvent;
import dev.slsk.internal.events.SoulseekClientDisconnectedEvent;
import dev.slsk.internal.messaging.handlers.DistributedMessageHandler;
import dev.slsk.internal.messaging.handlers.PeerMessageHandler;
import dev.slsk.internal.messaging.handlers.ServerMessageEvent;
import dev.slsk.internal.messaging.handlers.ServerMessageHandler;
import dev.slsk.internal.network.ConnectionFactory;
import dev.slsk.internal.network.DistributedConnectionManager;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.PeerConnectionManager;
import dev.slsk.internal.network.PeerEndpoint;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.search.ParsedSearchQuery;
import dev.slsk.internal.search.SearchInternal;
import dev.slsk.internal.search.SearchResponder;
import dev.slsk.internal.search.SearchTarget;
import dev.slsk.internal.transfer.TransferDirection;
import dev.slsk.internal.transfer.TransferInternal;
import dev.slsk.upload.Upload;
import dev.slsk.user.Username;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class EngineTest {
    private static final InetSocketAddress ENDPOINT = endpoint(46001);

    @Test
    void constructorsPreserveVersionOptionsAndInitialState() {
        assertThrows(IllegalArgumentException.class, () -> new SoulseekEngine(100));
        SoulseekClientOptions options = new SoulseekClientOptions();
        try (SoulseekEngine client = new SoulseekEngine(9999, options)) {
            assertEquals(170, client.getMajorVersion());
            assertEquals(9999, client.getMinorVersion());
            assertSame(options, client.getOptions());
            assertEquals(SoulseekClientState.DISCONNECTED, client.getState());
            assertNull(client.getUsername());
            assertNull(client.getIpEndpoint());
            assertNull(client.getIpAddress());
            assertNull(client.getPort());
            assertNull(client.getServerInfo().parentMinSpeed());
        }
    }

    @Test
    void endpointPropertiesReportWhatTheConnectionRecorded() {
        Fixture fixture = new Fixture();
        fixture.client.setIpEndpointForTest(ENDPOINT);

        assertEquals(ENDPOINT, fixture.client.getIpEndpoint());
        assertEquals(ENDPOINT.getAddress(), fixture.client.getIpAddress());
        assertEquals(ENDPOINT.getPort(), fixture.client.getPort());
        fixture.close();
    }

    /**
     * The engine used to project its transfer registries into {@code Transfer}
     * lists itself. The upload facet does it now, and the property that mattered
     * is its: what {@code all()} hands back is a snapshot, so a consumer holding
     * one is not reading a registry that mutates under it.
     *
     * <p>Downloads are no longer a projection of the engine's registry at all —
     * they come from the managed queue, which is asserted where it lives.
     */
    @Test
    void theUploadFacetProjectsASnapshotOfTheLiveRegistry() {
        Fixture fixture = new Fixture();
        Map<Integer, TransferInternal> uploads = new HashMap<>();
        uploads.put(2, new TransferInternal(TransferDirection.UPLOAD, "u", "upload", 2));
        fixture.client.setUploadsForTest(uploads);

        try (Soulseek slsk = DefaultSoulseek.over(fixture.client, "me", "secret")) {
            List<Upload> uploadSnapshot = slsk.uploads().all();
            assertEquals("upload", uploadSnapshot.getFirst().path());

            uploads.clear();
            assertEquals(1, uploadSnapshot.size());
            assertEquals(List.of(), slsk.uploads().all());
        }
    }

    @Test
    void stateChangesRaiseSourceEventsSynchronously() {
        Fixture fixture = new Fixture();
        List<String> order = new ArrayList<>();
        fixture.client
                .events()
                .on(
                        Kind.STATE_CHANGED,
                        (dev.slsk.internal.events.SoulseekClientStateChangedEvent eventData) ->
                                order.add("state:" + eventData.state()));
        fixture.client.events().on(Kind.CONNECTED, (Void eventData) -> order.add("connected"));
        fixture.client.events().on(Kind.LOGGED_IN, (Void eventData) -> order.add("logged"));
        AtomicReference<SoulseekClientDisconnectedEvent> disconnected = new AtomicReference<>();
        fixture.client
                .events()
                .on(
                        Kind.DISCONNECTED,
                        (dev.slsk.internal.events.SoulseekClientDisconnectedEvent eventData) ->
                                disconnected.set(eventData));

        fixture.client.changeState(SoulseekClientState.CONNECTED, "connected", null);
        fixture.client.changeState(SoulseekClientState.LOGGED_IN, "logged", null);
        RuntimeException cause = new RuntimeException("bye");
        fixture.client.changeState(SoulseekClientState.DISCONNECTED, "bye", cause);

        assertEquals(List.of("state:CONNECTED", "connected", "state:LOGGED_IN", "logged", "state:DISCONNECTED"), order);
        assertEquals("bye", disconnected.get().message());
        assertSame(cause, disconnected.get().exception());
        fixture.close();
    }

    @Test
    void disconnectUsesSourceReasonsCancelsSearchesAndRetainsDownloads() {
        Fixture fixture = new Fixture();
        fixture.client.setStateForTest(SoulseekClientState.LOGGED_IN);
        SearchInternal search = new SearchInternal(ParsedSearchQuery.fromText("query"), SearchTarget.getNetwork(), 1);
        Map<Integer, SearchInternal> searches = new HashMap<>();
        searches.put(1, search);
        fixture.client.setSearchesForTest(searches);
        Map<Integer, TransferInternal> downloads = new HashMap<>();
        downloads.put(1, new TransferInternal(TransferDirection.DOWNLOAD, "user", "file", 1));
        fixture.client.setDownloadsForTest(downloads);

        fixture.client.disconnect(null, new RuntimeException("cause"));

        assertEquals(SoulseekClientState.DISCONNECTED, fixture.client.getState());
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
        fixture.client
                .events()
                .on(
                        Kind.DOWNLOAD_DENIED,
                        (dev.slsk.internal.events.DownloadDeniedEvent eventData) -> denied.set(eventData));
        fixture.client
                .events()
                .on(
                        Kind.DOWNLOAD_FAILED,
                        (dev.slsk.internal.events.DownloadFailedEvent eventData) -> failed.set(eventData));

        fixture.peer.raiseDenied(new DownloadDeniedEvent("user", "file", "rejected"));
        assertInstanceOf(TransferRejectedException.class, first.settlement().failure());
        assertInstanceOf(TransferRejectedException.class, second.settlement().failure());
        assertEquals("rejected", denied.get().message());

        first = new TransferInternal(TransferDirection.DOWNLOAD, "user", "file", 3);
        second = new TransferInternal(TransferDirection.DOWNLOAD, "user", "file", 4);
        fixture.client.setDownloadsForTest(new HashMap<>(Map.of(
                3, first,
                4, second)));
        fixture.peer.raiseFailed(new DownloadFailedEvent("user", "file"));
        assertInstanceOf(
                TransferReportedFailedException.class, first.settlement().failure());
        assertInstanceOf(
                TransferReportedFailedException.class, second.settlement().failure());
        assertEquals("file", failed.get().filename());
        fixture.close();
    }

    @Test
    void serverEventsForwardUpdateInfoAndKickDisconnects() {
        Fixture fixture = new Fixture();
        AtomicReference<String> global = new AtomicReference<>();
        AtomicReference<ServerSessionInfo> serverInfo = new AtomicReference<>();
        AtomicInteger kicked = new AtomicInteger();
        fixture.client.events().on(Kind.GLOBAL_MESSAGE_RECEIVED, (String value) -> global.set(value));
        fixture.client
                .events()
                .on(
                        Kind.SERVER_INFO_RECEIVED,
                        (dev.slsk.internal.connection.ServerSessionInfo value) -> serverInfo.set(value));
        fixture.client.events().on(Kind.KICKED_FROM_SERVER, (Void value) -> kicked.incrementAndGet());

        fixture.server.raise(ServerMessageEvent.GLOBAL_MESSAGE_RECEIVED, "global");
        fixture.server.raise(ServerMessageEvent.SERVER_INFO_RECEIVED, new ServerSessionInfo(1, 2, 3, true));
        fixture.client.setStateForTest(SoulseekClientState.LOGGED_IN);
        fixture.server.raise(ServerMessageEvent.KICKED_FROM_SERVER, null);

        assertEquals("global", global.get());
        assertEquals(2, serverInfo.get().parentSpeedRatio());
        assertEquals(true, fixture.client.getServerInfo().supporter());
        assertEquals(1, kicked.get());
        assertEquals(SoulseekClientState.DISCONNECTED, fixture.client.getState());
        AtomicReference<SoulseekClientDisconnectedEvent> disconnect = new AtomicReference<>();
        fixture.client
                .events()
                .on(
                        Kind.DISCONNECTED,
                        (dev.slsk.internal.events.SoulseekClientDisconnectedEvent value) -> disconnect.set(value));
        fixture.client.setStateForTest(SoulseekClientState.CONNECTED);
        fixture.server.raise(ServerMessageEvent.KICKED_FROM_SERVER, null);
        assertInstanceOf(KickedFromServerException.class, disconnect.get().exception());
        fixture.close();
    }

    @Test
    void subsystemEventsForwardPayloadAndSubscriptionsCanBeClosed() {
        Fixture fixture = new Fixture();
        AtomicReference<DiagnosticMessage> diagnostic = new AtomicReference<>();
        Subscription subscription = fixture.client.events().on(Kind.DIAGNOSTIC_GENERATED, diagnostic::set);

        DiagnosticMessage expected = new DiagnosticMessage(
                dev.slsk.internal.diagnostics.DiagnosticSeverity.INFO, EngineTest.class.getName(), "message");
        fixture.search.publishDiagnostic(expected);
        assertSame(expected, diagnostic.get());

        subscription.close();
        diagnostic.set(null);
        fixture.search.publishDiagnostic(new DiagnosticMessage(
                dev.slsk.internal.diagnostics.DiagnosticSeverity.INFO, EngineTest.class.getName(), "after"));
        assertNull(diagnostic.get(), "a closed subscription receives nothing");

        AtomicReference<DistributedChildEvent> child = new AtomicReference<>();
        fixture.client
                .events()
                .on(
                        Kind.DISTRIBUTED_CHILD_ADDED,
                        (dev.slsk.internal.events.DistributedChildEvent value) -> child.set(value));
        DistributedChildEvent childArgs = new DistributedChildEvent("child", ENDPOINT);
        fixture.distributed.raise(DistributedConnectionManager.Kind.CHILD_ADDED, childArgs);
        assertSame(childArgs, child.get());
        fixture.close();
    }

    /**
     * A facet translating one of these runs on the read loop that raised it.
     * Before containment, one that threw took the connection with it.
     */
    @Test
    void aThrowingListenerIsContainedAndTheRestStillRun() {
        Fixture fixture = new Fixture();
        List<String> delivered = new ArrayList<>();
        fixture.client.events().on(Kind.DISTRIBUTED_CHILD_ADDED, value -> {
            throw new IllegalStateException("listener is broken");
        });
        fixture.client.events().on(Kind.DISTRIBUTED_CHILD_ADDED, value -> delivered.add("second"));

        fixture.distributed.raise(
                DistributedConnectionManager.Kind.CHILD_ADDED, new DistributedChildEvent("child", ENDPOINT));

        assertEquals(List.of("second"), delivered);
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

        // The engine used to assemble a DistributedNetworkInfo for this; the
        // Diagnostics facet reads the manager directly, so that is what the
        // mesh snapshot has to reflect.
        try (Soulseek slsk = DefaultSoulseek.over(fixture.client, "me", "secret")) {
            MeshState mesh = slsk.diagnostics().mesh();

            assertEquals(2, mesh.branchLevel());
            assertEquals(Optional.of(Username.of("root")), mesh.branchRoot());
            assertTrue(mesh.isBranchRoot());
            assertTrue(mesh.hasParent());
            assertEquals(Optional.of(Username.of("parent")), mesh.parent());
            assertEquals(List.of(Username.of("child")), mesh.children());
        }
    }

    /**
     * The ninety-four named add/remove methods collapsed into one kind and one
     * {@code Consumer}. Nothing may have been dropped in the collapse, so every
     * event the client used to name still has to be a kind.
     */
    @Test
    void everyClientEventSurvivedTheCollapseIntoAKind() {
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
            "UserCannotConnect",
            "UserStatisticsChanged",
            "UserStatusChanged"
        };
        Set<String> kinds = Arrays.stream(Kind.values()).map(Enum::name).collect(java.util.stream.Collectors.toSet());
        assertEquals(names.length, kinds.size(), "a kind was added or lost");
        for (String name : names) {
            assertTrue(kinds.contains(screamingCase(name)), name + " has no kind; the collapse dropped an event");
            assertFalse(hasMethod("add" + name + "Listener"), "the named registration should be gone");
        }
    }

    /** {@code RoomTickerAdded} to {@code ROOM_TICKER_ADDED}. */
    private static String screamingCase(String name) {
        return name.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toUpperCase(java.util.Locale.ROOT);
    }

    private static boolean hasMethod(String name) {
        for (Method method : SoulseekEngine.class.getMethods()) {
            if (method.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the failure a blocking call produced.
     *
     * <p>Took a future before the API became blocking; the calls now throw
     * directly, so it takes the call itself.
     */
    private static Throwable failure(org.junit.jupiter.api.function.Executable body) {
        try {
            body.execute();
        } catch (java.util.concurrent.CompletionException wrapped) {
            return wrapped.getCause() == null ? wrapped : wrapped.getCause();
        } catch (Throwable failure) {
            return failure;
        }
        throw new AssertionError("Expected operation to fail");
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
        private final DistributedMessageHandler distributedHandler = diagnosticProxy(DistributedMessageHandler.class);
        private final PeerConnectionManager peerManager = diagnosticProxy(PeerConnectionManager.class);
        private final SoulseekEngine client;

        private Fixture() {
            client = new SoulseekEngine(
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
                    null,
                    search.proxy,
                    new RecordingWaiter(),
                    new TokenFactory(),
                    null,
                    null,
                    new TokenBucket(1, Duration.ofMillis(100)),
                    new TokenBucket(1, Duration.ofMillis(100)));
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
            if (method.getName().equals("write")) {
                return null;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class PeerHandlerProbe {
        private java.util.function.Consumer<DownloadDeniedEvent> denied;
        private java.util.function.Consumer<DownloadFailedEvent> failed;
        private final PeerMessageHandler proxy = (PeerMessageHandler) Proxy.newProxyInstance(
                PeerMessageHandler.class.getClassLoader(), new Class<?>[] {PeerMessageHandler.class}, this::invoke);

        @SuppressWarnings("unchecked")
        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("subscribe") && arguments.length == 2) {
                if (arguments[0] == PeerMessageHandler.Kind.DOWNLOAD_DENIED) {
                    denied = (java.util.function.Consumer<DownloadDeniedEvent>) arguments[1];
                } else if (arguments[0] == PeerMessageHandler.Kind.DOWNLOAD_FAILED) {
                    failed = (java.util.function.Consumer<DownloadFailedEvent>) arguments[1];
                }
                return (dev.slsk.Subscription) () -> {};
            }
            return defaultValue(method.getReturnType());
        }

        private void raiseDenied(DownloadDeniedEvent eventData) {
            denied.accept(eventData);
        }

        private void raiseFailed(DownloadFailedEvent eventData) {
            failed.accept(eventData);
        }
    }

    private static final class ServerHandlerProbe {
        private final Map<ServerMessageEvent, java.util.function.Consumer<?>> listeners = new HashMap<>();
        private final ServerMessageHandler proxy = (ServerMessageHandler) Proxy.newProxyInstance(
                ServerMessageHandler.class.getClassLoader(), new Class<?>[] {ServerMessageHandler.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("subscribe") && arguments.length == 2) {
                listeners.put((ServerMessageEvent) arguments[0], (java.util.function.Consumer<?>) arguments[1]);
                return (dev.slsk.Subscription) () -> {};
            }
            return defaultValue(method.getReturnType());
        }

        @SuppressWarnings("unchecked")
        private <T> void raise(ServerMessageEvent event, T eventData) {
            ((java.util.function.Consumer<T>) listeners.get(event)).accept(eventData);
        }
    }

    private static final class SearchResponderProbe {
        private java.util.function.Consumer<dev.slsk.internal.diagnostics.DiagnosticMessage> diagnostic;
        private final SearchResponder proxy = (SearchResponder) Proxy.newProxyInstance(
                SearchResponder.class.getClassLoader(), new Class<?>[] {SearchResponder.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("subscribe") && arguments.length == 1) {
                diagnostic =
                        (java.util.function.Consumer<dev.slsk.internal.diagnostics.DiagnosticMessage>) arguments[0];
                return (dev.slsk.Subscription) () -> diagnostic = null;
            }
            return defaultValue(method.getReturnType());
        }

        private void publishDiagnostic(DiagnosticMessage eventData) {
            diagnostic.accept(eventData);
        }
    }

    private static final class DistributedManagerProbe {
        private final Map<DistributedConnectionManager.Kind, Object> eventListeners = new HashMap<>();
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
            if (method.getName().equals("subscribe") && arguments.length == 2) {
                eventListeners.put((DistributedConnectionManager.Kind) arguments[0], arguments[1]);
                return (dev.slsk.Subscription) () -> eventListeners.remove(arguments[0], arguments[1]);
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
                case "removeAndCloseAll" -> {
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
        private <T> void raise(DistributedConnectionManager.Kind kind, T eventData) {
            java.util.function.Consumer<T> listener = (java.util.function.Consumer<T>) eventListeners.get(kind);
            listener.accept(eventData);
        }
    }

    private static final class RecordingWaiter implements Waiter {
        @Override
        public Duration getDefaultTimeout() {
            return Duration.ofSeconds(5);
        }

        @Override
        public void cancel(dev.slsk.internal.common.WaitKey key) {}

        @Override
        public void cancelAll() {}

        @Override
        public void complete(dev.slsk.internal.common.WaitKey key) {}

        @Override
        public <T> void complete(dev.slsk.internal.common.WaitKey key, T result) {}

        @Override
        public boolean hasWait(dev.slsk.internal.common.WaitKey key) {
            return false;
        }

        @Override
        public void fail(dev.slsk.internal.common.WaitKey key, Throwable exception) {}

        @Override
        public void timeout(dev.slsk.internal.common.WaitKey key) {}

        @Override
        public <T> dev.slsk.internal.common.Wait<T> register(
                dev.slsk.internal.common.WaitKey key,
                Class<T> resultType,
                Duration timeout,
                CancellationSignal cancellationSignal) {
            // Answers at once with nothing. No test here provokes a correlated
            // request; a wait that never settles would hang one that did.
            return () -> null;
        }

        @Override
        public void close() {}
    }
}
