// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.handlers;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationSignal;
import dev.slsk.common.Constants;
import dev.slsk.common.WaitKey;
import dev.slsk.common.Waiter;
import dev.slsk.diagnostics.DiagnosticSink;
import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.messages.DistributedBranchLevel;
import dev.slsk.messaging.messages.DistributedBranchRoot;
import dev.slsk.messaging.messages.DistributedChildDepth;
import dev.slsk.messaging.messages.DistributedPingRequest;
import dev.slsk.messaging.messages.DistributedPingResponse;
import dev.slsk.messaging.messages.DistributedSearchRequest;
import dev.slsk.messaging.messages.EmbeddedMessage;
import dev.slsk.messaging.messages.OutgoingMessage;
import dev.slsk.network.DistributedConnectionManager;
import dev.slsk.network.MessageConnection;
import dev.slsk.network.MessageEvent;
import dev.slsk.network.PeerEndpoint;
import dev.slsk.network.tcp.ConnectionKey;
import dev.slsk.network.tcp.ConnectionState;
import dev.slsk.network.tcp.ConnectionTypes;
import dev.slsk.options.SoulseekClientOptions;
import dev.slsk.options.SoulseekClientOptionsPatch;
import dev.slsk.search.SearchResponder;
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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DistributedMessageHandlerTest {
    private static final String LOCAL_USER = "local";
    private static final String USERNAME = "peer";
    private static final InetSocketAddress ENDPOINT = endpoint(43001);
    private static final int TOKEN = 0x10203040;

    @Test
    void constructionRequiresClient() {
        assertThrows(NullPointerException.class, () -> new DefaultDistributedMessageHandler(null));
    }

    @Test
    void branchMessagesUpdateOnlyCurrentParent() {
        Fixture fixture = new Fixture(true);
        ConnectionProbe parent = new ConnectionProbe(USERNAME, ENDPOINT);
        fixture.manager.parent = new PeerEndpoint(USERNAME, ENDPOINT);

        fixture.handler
                .handleMessageReadAsync(parent.proxy, new DistributedBranchLevel(3).toByteArray())
                .join();
        fixture.handler
                .handleMessageReadAsync(parent.proxy, new DistributedBranchRoot("root").toByteArray())
                .join();

        assertEquals(3, fixture.manager.branchLevel);
        assertEquals("root", fixture.manager.branchRoot);

        ConnectionProbe other = new ConnectionProbe("other", endpoint(43002));
        fixture.handler
                .handleMessageReadAsync(other.proxy, new DistributedBranchLevel(9).toByteArray())
                .join();
        assertEquals(3, fixture.manager.branchLevel);
    }

    @Test
    void childDepthAndPingCompleteExpectedWaitKeys() {
        Fixture fixture = new Fixture(true);
        ConnectionProbe connection = new ConnectionProbe(USERNAME, ENDPOINT);
        CompletableFuture<Integer> depth = fixture.waiter.waitAsync(
                new WaitKey(Constants.WaitKey.CHILD_DEPTH_MESSAGE, connection.key), Integer.class);
        CompletableFuture<DistributedPingResponse> ping = fixture.waiter.waitAsync(
                new WaitKey(MessageCode.Distributed.PING, USERNAME), DistributedPingResponse.class);

        fixture.handler
                .handleMessageReadAsync(connection.proxy, new DistributedChildDepth(7).toByteArray())
                .join();
        fixture.handler
                .handleMessageReadAsync(connection.proxy, new DistributedPingResponse(TOKEN).toByteArray())
                .join();

        assertEquals(7, depth.join());
        assertEquals(TOKEN, ping.join().getToken());
    }

    @Test
    void searchIsBroadcastRespondedAndDeduplicated() {
        Fixture fixture = new Fixture(true);
        ConnectionProbe connection = new ConnectionProbe("parent", ENDPOINT);
        byte[] search = new DistributedSearchRequest(USERNAME, TOKEN, "query").toByteArray();

        fixture.handler.handleMessageReadAsync(connection.proxy, search).join();
        fixture.handler.handleMessageReadAsync(connection.proxy, search).join();

        assertEquals(1, fixture.manager.broadcasts.size());
        assertArrayEquals(search, fixture.manager.broadcasts.getFirst());
        assertEquals(1, fixture.responder.calls.size());
        assertEquals(new SearchCall(USERNAME, TOKEN, "query"), fixture.responder.calls.getFirst());
    }

    @Test
    void deduplicationCanBeDisabledAndOwnSearchIsNotAnswered() {
        Fixture fixture = new Fixture(false);
        ConnectionProbe connection = new ConnectionProbe("parent", ENDPOINT);
        byte[] other = new DistributedSearchRequest(USERNAME, TOKEN, "query").toByteArray();
        fixture.handler.handleMessageReadAsync(connection.proxy, other).join();
        fixture.handler.handleMessageReadAsync(connection.proxy, other).join();
        assertEquals(2, fixture.responder.calls.size());

        byte[] own = new DistributedSearchRequest(LOCAL_USER, TOKEN + 1, "own").toByteArray();
        fixture.handler.handleMessageReadAsync(connection.proxy, own).join();
        assertEquals(2, fixture.responder.calls.size());
        assertEquals(3, fixture.manager.broadcasts.size());
    }

    @Test
    void embeddedParentSearchIsUnwrappedBeforeBroadcast() {
        Fixture fixture = new Fixture(true);
        ConnectionProbe connection = new ConnectionProbe("root", ENDPOINT);
        byte[] embedded = embeddedSearch(USERNAME, TOKEN, "query");
        byte[] expected = EmbeddedMessage.fromByteArray(embedded).getDistributedMessage();

        fixture.handler.handleMessageReadAsync(connection.proxy, embedded).join();

        assertArrayEquals(expected, fixture.manager.broadcasts.getFirst());
        assertEquals(new SearchCall(USERNAME, TOKEN, "query"), fixture.responder.calls.getFirst());
    }

    @Test
    void serverEmbeddedSearchPromotesBroadcastsAndResponds() {
        Fixture fixture = new Fixture(true);
        byte[] embedded = embeddedSearch(USERNAME, TOKEN, "query");
        byte[] expected = EmbeddedMessage.fromByteArray(embedded).getDistributedMessage();

        fixture.handler.handleEmbeddedMessageAsync(embedded).join();

        assertEquals(1, fixture.manager.promotions);
        assertArrayEquals(expected, fixture.manager.broadcasts.getFirst());
        assertEquals(new SearchCall(USERNAME, TOKEN, "query"), fixture.responder.calls.getFirst());
    }

    @Test
    void childPingWritesResponseWithNextToken() {
        Fixture fixture = new Fixture(true);
        ConnectionProbe child = new ConnectionProbe(USERNAME, ENDPOINT);

        fixture.handler
                .handleChildMessageReadAsync(child.proxy, new DistributedPingRequest().toByteArray())
                .join();

        assertEquals(1, child.outgoing.size());
        DistributedPingResponse response = assertInstanceOf(DistributedPingResponse.class, child.outgoing.getFirst());
        assertEquals(TOKEN, response.getToken());
    }

    @Test
    void failuresAndUnhandledMessagesProduceSourceDiagnostics() {
        Fixture fixture = new Fixture(true);
        ConnectionProbe child = new ConnectionProbe(USERNAME, ENDPOINT);
        child.writeFuture = CompletableFuture.failedFuture(new RuntimeException("write"));
        fixture.handler
                .handleChildMessageReadAsync(child.proxy, new DistributedPingRequest().toByteArray())
                .join();
        assertTrue(fixture.diagnostic.containsWarning("Error handling distributed child message"));

        fixture.handler
                .handleMessageReadAsync(
                        child.proxy,
                        new MessageBuilder()
                                .writeCode(MessageCode.Distributed.UNKNOWN)
                                .build())
                .join();
        assertTrue(fixture.diagnostic.contains("Unhandled distributed message"));

        fixture.handler.handleEmbeddedMessageAsync(new byte[] {1}).join();
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
                null, null, null, null, null, null, null, null);
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

    private static final class Fixture {
        private final RecordingDiagnostic diagnostic = new RecordingDiagnostic();
        private final FakeWaiter waiter = new FakeWaiter();
        private final ManagerProbe manager = new ManagerProbe();
        private final ResponderProbe responder = new ResponderProbe();
        private final FakeClient client;
        private final DefaultDistributedMessageHandler handler;

        private Fixture(boolean deduplicate) {
            client = new FakeClient(options(deduplicate), waiter, manager.proxy, responder.proxy);
            handler = new DefaultDistributedMessageHandler(client, diagnostic);
        }
    }

    private static final class FakeClient implements DistributedMessageHandlerClient {
        private final SoulseekClientOptions options;
        private final Waiter waiter;
        private final DistributedConnectionManager manager;
        private final SearchResponder responder;
        private final AtomicInteger token = new AtomicInteger(TOKEN);

        private FakeClient(
                SoulseekClientOptions options,
                Waiter waiter,
                DistributedConnectionManager manager,
                SearchResponder responder) {
            this.options = options;
            this.waiter = waiter;
            this.manager = manager;
            this.responder = responder;
        }

        @Override
        public SoulseekClientOptions getOptions() {
            return options;
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
        public Waiter getWaiter() {
            return waiter;
        }

        @Override
        public DistributedConnectionManager getDistributedConnectionManager() {
            return manager;
        }

        @Override
        public SearchResponder getSearchResponder() {
            return responder;
        }
    }

    private static final class ManagerProbe implements InvocationHandler {
        private final DistributedConnectionManager proxy = (DistributedConnectionManager) Proxy.newProxyInstance(
                DistributedConnectionManager.class.getClassLoader(),
                new Class<?>[] {DistributedConnectionManager.class},
                this);
        private final List<byte[]> broadcasts = new ArrayList<>();
        private PeerEndpoint parent = new PeerEndpoint("", null);
        private int branchLevel;
        private String branchRoot;
        private int promotions;

        @Override
        public Object invoke(Object ignored, Method method, Object[] arguments) {
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
                case "broadcastMessageAsync" -> {
                    byte[] bytes = (byte[]) arguments[0];
                    broadcasts.add(Arrays.copyOf(bytes, bytes.length));
                    yield CompletableFuture.completedFuture(null);
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
        private final List<SearchCall> calls = new ArrayList<>();

        @Override
        public Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("tryRespondAsync") && arguments.length == 3) {
                calls.add(new SearchCall((String) arguments[0], (Integer) arguments[1], (String) arguments[2]));
                return CompletableFuture.completedFuture(true);
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class FakeWaiter implements Waiter {
        private final Map<WaitKey, CompletableFuture<?>> waits = new HashMap<>();

        @Override
        public int getDefaultTimeout() {
            return 5_000;
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

        @Override
        public CompletableFuture<Void> waitAsync(WaitKey key) {
            return waitAsync(key, Void.class, null, CancellationSignal.none());
        }

        @Override
        public CompletableFuture<Void> waitAsync(WaitKey key, Integer timeout) {
            return waitAsync(key, Void.class, timeout, CancellationSignal.none());
        }

        @Override
        public CompletableFuture<Void> waitAsync(WaitKey key, Integer timeout, CancellationSignal cancellationSignal) {
            return waitAsync(key, Void.class, timeout, cancellationSignal);
        }

        @Override
        public <T> CompletableFuture<T> waitAsync(WaitKey key, Class<T> resultType) {
            return waitAsync(key, resultType, null, CancellationSignal.none());
        }

        @Override
        public <T> CompletableFuture<T> waitAsync(WaitKey key, Class<T> resultType, Integer timeout) {
            return waitAsync(key, resultType, timeout, CancellationSignal.none());
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> CompletableFuture<T> waitAsync(
                WaitKey key, Class<T> resultType, Integer timeout, CancellationSignal cancellationSignal) {
            return (CompletableFuture<T>) waits.computeIfAbsent(key, ignored -> new CompletableFuture<>());
        }

        @Override
        public CompletableFuture<Void> waitIndefinitelyAsync(WaitKey key) {
            return waitAsync(key);
        }

        @Override
        public CompletableFuture<Void> waitIndefinitelyAsync(WaitKey key, CancellationSignal cancellationSignal) {
            return waitAsync(key, null, cancellationSignal);
        }

        @Override
        public <T> CompletableFuture<T> waitIndefinitelyAsync(WaitKey key, Class<T> resultType) {
            return waitAsync(key, resultType);
        }

        @Override
        public <T> CompletableFuture<T> waitIndefinitelyAsync(
                WaitKey key, Class<T> resultType, CancellationSignal cancellationSignal) {
            return waitAsync(key, resultType, null, cancellationSignal);
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
        public Object invoke(Object ignored, Method method, Object[] arguments) {
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
                case "writeAsync" -> {
                    if (arguments[0] instanceof OutgoingMessage message) {
                        outgoing.add(message);
                    }
                    yield writeFuture;
                }
                case "toString" -> "ConnectionProbe";
                default -> defaultValue(method.getReturnType());
            };
        }
    }

    private static final class RecordingDiagnostic implements DiagnosticSink {
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
