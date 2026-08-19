// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.ServerLink;
import dev.slsk.internal.ServerLinks;
import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.Eventually;
import dev.slsk.internal.common.Outcomes;
import dev.slsk.internal.common.TokenFactory;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.messages.DistributedBranchLevel;
import dev.slsk.internal.messaging.messages.DistributedBranchRoot;
import dev.slsk.internal.messaging.messages.DistributedChildDepth;
import dev.slsk.internal.messaging.messages.DistributedPingRequest;
import dev.slsk.internal.messaging.messages.DistributedPingResponse;
import dev.slsk.internal.messaging.messages.DistributedSearchRequest;
import dev.slsk.internal.messaging.messages.EmbeddedMessage;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.network.DistributedConnectionManager;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.MessageEvent;
import dev.slsk.internal.network.PeerEndpoint;
import dev.slsk.internal.network.tcp.ConnectionKey;
import dev.slsk.internal.network.tcp.ConnectionState;
import dev.slsk.internal.network.tcp.ConnectionTypes;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.options.SoulseekClientOptionsPatch;
import dev.slsk.internal.search.SearchResponder;
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
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

class DistributedMessageHandlerTest {
    private static final String LOCAL_USER = "local";
    private static final String USERNAME = "peer";
    private static final InetSocketAddress ENDPOINT = endpoint(43001);
    private static final int TOKEN = 0x10203040;

    @Test
    void constructionRequiresItsPorts() {
        Fixture fixture = new Fixture(true);
        assertThrows(
                NullPointerException.class,
                () -> new DefaultDistributedMessageHandler(
                        null,
                        fixture.server,
                        new TokenFactory(TOKEN),
                        fixture.waiter,
                        () -> fixture.manager.proxy,
                        () -> fixture.responder.proxy));
    }

    @Test
    void branchMessagesUpdateOnlyCurrentParent() {
        Fixture fixture = new Fixture(true);
        ConnectionProbe parent = new ConnectionProbe(USERNAME, ENDPOINT);
        fixture.manager.parent = new PeerEndpoint(USERNAME, ENDPOINT);

        fixture.handler.handleMessageRead(parent.proxy, new DistributedBranchLevel(3).toByteArray());
        fixture.handler.handleMessageRead(parent.proxy, new DistributedBranchRoot("root").toByteArray());

        assertEquals(3, fixture.manager.branchLevel);
        assertEquals("root", fixture.manager.branchRoot);

        ConnectionProbe other = new ConnectionProbe("other", endpoint(43002));
        fixture.handler.handleMessageRead(other.proxy, new DistributedBranchLevel(9).toByteArray());
        assertEquals(3, fixture.manager.branchLevel);
    }

    @Test
    void childDepthAndPingCompleteExpectedWaitKeys() throws Exception {
        Fixture fixture = new Fixture(true);
        ConnectionProbe connection = new ConnectionProbe(USERNAME, ENDPOINT);
        Wait<Integer> depth = fixture.waiter.register(
                new WaitKey(Constants.WaitKey.CHILD_DEPTH_MESSAGE, connection.key),
                Integer.class,
                fixture.waiter.getDefaultTimeout(),
                null);
        Wait<DistributedPingResponse> ping = fixture.waiter.register(
                new WaitKey(MessageCode.Distributed.PING, USERNAME),
                DistributedPingResponse.class,
                fixture.waiter.getDefaultTimeout(),
                null);

        fixture.handler.handleMessageRead(connection.proxy, new DistributedChildDepth(7).toByteArray());
        fixture.handler.handleMessageRead(connection.proxy, new DistributedPingResponse(TOKEN).toByteArray());

        assertEquals(7, depth.await());
        assertEquals(TOKEN, ping.await().getToken());
    }

    @Test
    void searchIsBroadcastRespondedAndDeduplicated() {
        Fixture fixture = new Fixture(true);
        ConnectionProbe connection = new ConnectionProbe("parent", ENDPOINT);
        byte[] search = new DistributedSearchRequest(USERNAME, TOKEN, "query").toByteArray();

        fixture.handler.handleMessageRead(connection.proxy, search);
        fixture.handler.handleMessageRead(connection.proxy, search);

        awaitBroadcasts(fixture, 1);
        awaitResponses(fixture, 1);
        assertArrayEquals(search, fixture.manager.broadcasts.getFirst());
        assertEquals(new SearchCall(USERNAME, TOKEN, "query"), fixture.responder.calls.getFirst());
    }

    @Test
    void deduplicationCanBeDisabledAndOwnSearchIsNotAnswered() {
        Fixture fixture = new Fixture(false);
        ConnectionProbe connection = new ConnectionProbe("parent", ENDPOINT);
        byte[] other = new DistributedSearchRequest(USERNAME, TOKEN, "query").toByteArray();
        fixture.handler.handleMessageRead(connection.proxy, other);
        fixture.handler.handleMessageRead(connection.proxy, other);
        awaitResponses(fixture, 2);

        byte[] own = new DistributedSearchRequest(LOCAL_USER, TOKEN + 1, "own").toByteArray();
        fixture.handler.handleMessageRead(connection.proxy, own);
        awaitBroadcasts(fixture, 3);
        assertEquals(2, fixture.responder.calls.size());
    }

    @Test
    void embeddedParentSearchIsUnwrappedBeforeBroadcast() {
        Fixture fixture = new Fixture(true);
        ConnectionProbe connection = new ConnectionProbe("root", ENDPOINT);
        byte[] embedded = embeddedSearch(USERNAME, TOKEN, "query");
        byte[] expected = EmbeddedMessage.fromByteArray(embedded).getDistributedMessage();

        fixture.handler.handleMessageRead(connection.proxy, embedded);

        awaitBroadcasts(fixture, 1);
        awaitResponses(fixture, 1);
        assertArrayEquals(expected, fixture.manager.broadcasts.getFirst());
        assertEquals(new SearchCall(USERNAME, TOKEN, "query"), fixture.responder.calls.getFirst());
    }

    @Test
    void serverEmbeddedSearchPromotesBroadcastsAndResponds() {
        Fixture fixture = new Fixture(true);
        byte[] embedded = embeddedSearch(USERNAME, TOKEN, "query");
        byte[] expected = EmbeddedMessage.fromByteArray(embedded).getDistributedMessage();

        fixture.handler.handleEmbeddedMessage(embedded);

        assertEquals(1, fixture.manager.promotions);
        awaitBroadcasts(fixture, 1);
        awaitResponses(fixture, 1);
        assertArrayEquals(expected, fixture.manager.broadcasts.getFirst());
        assertEquals(new SearchCall(USERNAME, TOKEN, "query"), fixture.responder.calls.getFirst());
    }

    @Test
    void childPingWritesResponseWithNextToken() {
        Fixture fixture = new Fixture(true);
        ConnectionProbe child = new ConnectionProbe(USERNAME, ENDPOINT);

        fixture.handler.handleChildMessageRead(child.proxy, new DistributedPingRequest().toByteArray());

        // The ping answer goes to a thread of its own, as the dispatched write
        // it replaces did: answering in front of the next search request delays
        // every child behind this one.
        assertTrue(Eventually.holds(() -> child.outgoing.size() == 1));
        DistributedPingResponse response = assertInstanceOf(DistributedPingResponse.class, child.outgoing.getFirst());
        assertEquals(TOKEN, response.getToken());
    }

    @Test
    void failuresAndUnhandledMessagesProduceSourceDiagnostics() {
        Fixture fixture = new Fixture(true);
        ConnectionProbe child = new ConnectionProbe(USERNAME, ENDPOINT);
        child.writeFuture = CompletableFuture.failedFuture(new RuntimeException("write"));
        fixture.handler.handleChildMessageRead(child.proxy, new DistributedPingRequest().toByteArray());
        // The dispatched write reports its own failure now; the future that
        // used to carry it back to this handler is gone.
        assertTrue(
                Eventually.holds(() -> fixture.diagnostic.containsWarning("Error handling distributed child message")));

        fixture.handler.handleMessageRead(
                child.proxy,
                new MessageBuilder().writeCode(MessageCode.Distributed.UNKNOWN).build());
        assertTrue(fixture.diagnostic.contains("Unhandled distributed message"));

        fixture.handler.handleEmbeddedMessage(new byte[] {1});
        assertTrue(fixture.diagnostic.containsWarning("Error handling embedded message"));
    }

    @Test
    void writtenMessageCallbacksLogNonPingCodes() {
        Fixture fixture = new Fixture(true);
        ConnectionProbe connection = new ConnectionProbe(USERNAME, ENDPOINT);
        MessageEvent branch = new MessageEvent(new DistributedBranchLevel(1).toByteArray());
        fixture.handler.handleChildMessageWritten(connection.proxy, branch);
        fixture.handler.handleMessageWritten(connection.proxy, branch);

        assertTrue(fixture.diagnostic.contains("Distributed child message sent"));
        assertTrue(fixture.diagnostic.contains("Distributed message sent"));
    }

    private static byte[] embeddedSearch(String username, int token, String query) {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.EMBEDDED_MESSAGE)
                .writeByte(0x03)
                .writeBytes(new byte[4])
                .writeString(username)
                .writeInteger(token)
                .writeString(query)
                .build();
    }

    private static SoulseekClientOptions options(boolean deduplicate) {
        if (deduplicate) {
            return new SoulseekClientOptions();
        }
        SoulseekClientOptionsPatch patch = new SoulseekClientOptionsPatch(
                null, null, null, null, null, null, null, null, false, null, null, null, null, null, null, null, null,
                null, null);
        return new SoulseekClientOptions().with(patch);
    }

    private static InetSocketAddress endpoint(int port) {
        try {
            return new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port);
        } catch (Exception exception) {
            throw new AssertionError(exception);
        }
    }

    private record SearchCall(String username, int token, String query) {}

    /**
     * Waits for the dispatched broadcasts to arrive.
     *
     * <p>The handler puts a broadcast on a thread of its own — fanning out to
     * every child is not the parent read loop's to wait for — so the probe
     * records it after the call that provoked it has returned.
     */
    private static void awaitBroadcasts(Fixture fixture, int count) {
        assertTrue(Eventually.holds(() -> fixture.manager.broadcasts.size() >= count));
        assertEquals(count, fixture.manager.broadcasts.size());
    }

    /** Waits for the dispatched search responses, for the same reason. */
    private static void awaitResponses(Fixture fixture, int count) {
        assertTrue(Eventually.holds(() -> fixture.responder.calls.size() >= count));
        assertEquals(count, fixture.responder.calls.size());
    }

    private static final class Fixture {
        private final RecordingDiagnostic diagnostic = new RecordingDiagnostic();
        private final FakeWaiter waiter = new FakeWaiter();
        private final ManagerProbe manager = new ManagerProbe();
        private final ResponderProbe responder = new ResponderProbe();
        private final ServerLink server = ServerLinks.loggedIn(waiter, diagnostic, null, LOCAL_USER);
        private final DefaultDistributedMessageHandler handler;

        private Fixture(boolean deduplicate) {
            SoulseekClientOptions options = options(deduplicate);
            handler = new DefaultDistributedMessageHandler(
                    () -> options,
                    server,
                    new TokenFactory(TOKEN),
                    waiter,
                    () -> manager.proxy,
                    () -> responder.proxy,
                    diagnostic);
        }
    }

    private static final class ManagerProbe implements InvocationHandler {
        private final DistributedConnectionManager proxy = (DistributedConnectionManager) Proxy.newProxyInstance(
                DistributedConnectionManager.class.getClassLoader(),
                new Class<?>[] {DistributedConnectionManager.class},
                this);
        private final List<byte[]> broadcasts = new CopyOnWriteArrayList<>();
        private PeerEndpoint parent = new PeerEndpoint("", null);
        private int branchLevel;
        private String branchRoot;
        private int promotions;

        @Override
        public Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            return switch (method.getName()) {
                case "getParent" -> parent;
                case "setParentBranchLevel" -> {
                    branchLevel = (Integer) arguments[0];
                    yield null;
                }
                case "setParentBranchRoot" -> {
                    branchRoot = (String) arguments[0];
                    yield null;
                }
                case "broadcastMessage" -> {
                    byte[] bytes = (byte[]) arguments[0];
                    broadcasts.add(Arrays.copyOf(bytes, bytes.length));
                    yield null;
                }
                case "promoteToBranchRoot" -> {
                    promotions++;
                    yield null;
                }
                case "toString" -> "ManagerProbe";
                default -> defaultValue(method.getReturnType());
            };
        }
    }

    private static final class ResponderProbe implements InvocationHandler {
        private final SearchResponder proxy = (SearchResponder) Proxy.newProxyInstance(
                SearchResponder.class.getClassLoader(), new Class<?>[] {SearchResponder.class}, this);
        private final List<SearchCall> calls = new CopyOnWriteArrayList<>();

        @Override
        public Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            if (method.getName().equals("tryRespond") && arguments.length == 3) {
                calls.add(new SearchCall((String) arguments[0], (Integer) arguments[1], (String) arguments[2]));
                return true;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class FakeWaiter implements Waiter {
        private final Map<WaitKey, CompletableFuture<?>> waits = new HashMap<>();

        @Override
        public Duration getDefaultTimeout() {
            return Duration.ofSeconds(5);
        }

        @Override
        public void cancel(WaitKey key) {
            CompletableFuture<?> wait = waits.remove(key);
            if (wait != null) {
                wait.cancel(false);
            }
        }

        @Override
        public void cancelAll() {
            waits.values().forEach(wait -> wait.cancel(false));
            waits.clear();
        }

        @Override
        public void complete(WaitKey key) {
            complete(key, null);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> void complete(WaitKey key, T result) {
            CompletableFuture<T> wait = (CompletableFuture<T>) waits.get(key);
            if (wait != null) {
                wait.complete(result);
            }
        }

        @Override
        public boolean hasWait(WaitKey key) {
            return waits.containsKey(key);
        }

        @Override
        public void fail(WaitKey key, Throwable exception) {
            CompletableFuture<?> wait = waits.get(key);
            if (wait != null) {
                wait.completeExceptionally(exception);
            }
        }

        @Override
        public void timeout(WaitKey key) {
            fail(key, new java.util.concurrent.TimeoutException());
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> Wait<T> register(
                WaitKey key, Class<T> resultType, Duration timeout, CancellationSignal cancellationSignal) {
            CompletableFuture<T> wait =
                    (CompletableFuture<T>) waits.computeIfAbsent(key, ignored -> new CompletableFuture<>());
            // The future is how a test says what the answer will be; Outcomes
            // turns it into what a real wait raises.
            return () -> Outcomes.raise(wait);
        }

        @Override
        public void close() {
            cancelAll();
        }
    }

    private static final class ConnectionProbe implements InvocationHandler {
        private final String username;
        private final InetSocketAddress endpoint;
        private final UUID id = UUID.randomUUID();
        private final ConnectionKey key;
        private final MessageConnection proxy;
        private final List<OutgoingMessage> outgoing = new ArrayList<>();
        private CompletableFuture<Void> writeFuture = CompletableFuture.completedFuture(null);

        private ConnectionProbe(String username, InetSocketAddress endpoint) {
            this.username = username;
            this.endpoint = endpoint;
            key = new ConnectionKey(username, endpoint);
            proxy = (MessageConnection) Proxy.newProxyInstance(
                    MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this);
        }

        @Override
        public Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            return switch (method.getName()) {
                case "getUsername" -> username;
                case "getIpEndpoint" -> endpoint;
                case "getId" -> id;
                case "getKey" -> key;
                case "getInactiveTime" -> Duration.ZERO;
                case "getState" -> ConnectionState.CONNECTED;
                case "getType" -> ConnectionTypes.NONE;
                case "getWriteQueueDepth" -> 0;
                case "getCodeLength" -> 1;
                case "isServerConnection", "isReadingContinuously" -> false;
                case "write" -> {
                    if (arguments[0] instanceof OutgoingMessage message) {
                        outgoing.add(message);
                    }
                    Outcomes.raise(writeFuture);
                    yield null;
                }
                case "toString" -> "ConnectionProbe";
                default -> defaultValue(method.getReturnType());
            };
        }
    }

    private static final class RecordingDiagnostic implements DiagnosticSink {
        // Copy-on-write: a handler's dispatched work reports its own failures
        // now, from a thread of its own, while the test thread is reading.
        private final List<String> messages = new CopyOnWriteArrayList<>();
        private final List<String> warnings = new CopyOnWriteArrayList<>();

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
        if (type == float.class) {
            return 0f;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == char.class) {
            return '\0';
        }
        return null;
    }
}
