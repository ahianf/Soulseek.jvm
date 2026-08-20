// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.exceptions.AddressException;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.ConnectionWriteException;
import dev.slsk.exceptions.ListenException;
import dev.slsk.exceptions.LoginRejectedException;
import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.common.Outcomes;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.concurrent.CancellationController;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.messages.LoginRequest;
import dev.slsk.internal.messaging.messages.LoginResponse;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.messaging.messages.PrivateRoomToggle;
import dev.slsk.internal.messaging.messages.SetListenPortCommand;
import dev.slsk.internal.network.ConnectionFactory;
import dev.slsk.internal.network.DistributedConnectionManager;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.MessageEvent;
import dev.slsk.internal.network.tcp.ConnectionDisconnectedEvent;
import dev.slsk.internal.network.tcp.TransportConnection;
import dev.slsk.internal.options.SoulseekClientOptions;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class EngineConnectTest {
    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();

    @Test
    void validatesArgumentsAndCurrentStateInSourceOrder() {
        Fixture fixture = new Fixture();
        for (String[] credentials : new String[][] {
            {null, "password"},
            {"", "password"},
            {"user", null},
            {"user", ""}
        }) {
            assertThrows(IllegalArgumentException.class, () -> fixture.client.connect(credentials[0], credentials[1]));
        }
        for (String invalid : new String[] {null, "", " ", "\t"}) {
            assertThrows(IllegalArgumentException.class, () -> fixture.client.connect(invalid, 1, "user", "password"));
        }
        assertThrows(IllegalArgumentException.class, () -> fixture.client.connect("127.0.0.1", -1, "user", "password"));
        assertThrows(
                IllegalArgumentException.class, () -> fixture.client.connect("127.0.0.1", 65_536, "user", "password"));

        fixture.client.setStateForTest(SoulseekClientState.CONNECTING);
        assertThrows(IllegalStateException.class, () -> fixture.client.connect("127.0.0.1", 1, "user", "password"));
        fixture.client.setStateForTest(SoulseekClientState.LOGGING_IN);
        assertThrows(IllegalStateException.class, () -> fixture.client.connect("127.0.0.1", 1, "user", "password"));
        fixture.client.setStateForTest(SoulseekClientState.CONNECTED);
        assertThrows(IllegalStateException.class, () -> fixture.client.connect("127.0.0.1", 1, "user", "password"));
        fixture.client.setStateForTest(SoulseekClientState.DISCONNECTED);
        fixture.close();
    }

    @Test
    void wrapsAddressResolutionFailure() {
        Fixture fixture = new Fixture();
        AddressException failure = assertThrows(
                AddressException.class, () -> fixture.client.connect("not-a-host.invalid", 2271, "user", "password"));
        assertInstanceOf(java.net.UnknownHostException.class, failure.getCause());
        fixture.close();
    }

    @Test
    void listenerPreflightFailureIsReportedBeforeConnecting() throws Exception {
        InetAddress nonLocalAddress = InetAddress.getByAddress(new byte[] {(byte) 192, 0, 2, 1});
        SoulseekClientOptions options =
                SoulseekClientOptions.builder().listenIpAddress(nonLocalAddress).build();
        Fixture fixture = new Fixture(options);
        assertThrows(ListenException.class, () -> fixture.client.connect("127.0.0.1", 2271, "user", "password"));
        assertEquals(0, fixture.connection.connectCount);
        fixture.close();
    }

    @Test
    void connectsLogsInAndSendsConfigurationInOrder() {
        Fixture fixture = new Fixture();
        CancellationController source = new CancellationController();
        CancellationSignal token = source.getSignal();
        List<SoulseekClientState> states = new ArrayList<>();
        fixture.client
                .events()
                .on(
                        Kind.STATE_CHANGED,
                        (dev.slsk.internal.events.SoulseekClientStateChangedEvent eventData) ->
                                states.add(eventData.state()));
        fixture.connection.fireConnected = true;

        fixture.client.connect("127.0.0.1", 2271, "alice", "secret", token);

        assertEquals(1, fixture.connection.connectCount);
        assertSame(token, fixture.connection.connectToken);
        assertEquals(new InetSocketAddress(LOOPBACK, 2271), fixture.factory.endpoint);
        assertSame(fixture.options.serverConnectionOptions(), fixture.factory.options);
        assertEquals("127.0.0.1", fixture.client.getAddress());
        assertEquals(new InetSocketAddress(LOOPBACK, 2271), fixture.client.getIpEndpoint());
        assertEquals("alice", fixture.client.getUsername());
        assertEquals(loggedIn(), fixture.client.getState());

        ByteArrayOutputStream expectedLogin = new ByteArrayOutputStream();
        expectedLogin.writeBytes(new LoginRequest(9999, "alice", "secret").toByteArray());
        expectedLogin.writeBytes(new SetListenPortCommand(fixture.options.listenPort()).toByteArray());
        assertArrayEquals(expectedLogin.toByteArray(), fixture.connection.rawMessages.get(0));
        assertInstanceOf(SetListenPortCommand.class, fixture.connection.outgoingMessages.get(0));
        PrivateRoomToggle toggle =
                assertInstanceOf(PrivateRoomToggle.class, fixture.connection.outgoingMessages.get(1));
        assertEquals(fixture.options.acceptPrivateRoomInvitations(), toggle.isAcceptInvitations());
        assertEquals(
                "alice",
                assertInstanceOf(
                                dev.slsk.internal.messaging.messages.UserStatisticsRequest.class,
                                fixture.connection.outgoingMessages.get(2))
                        .getUsername(),
                "login asks for our own statistics, which seed the advertised upload speed");
        assertEquals(1, fixture.distributed.updateCount);
        assertSame(token, fixture.distributed.updateToken);
        assertEquals(
                List.of(
                        SoulseekClientState.CONNECTING,
                        SoulseekClientState.CONNECTED,
                        SoulseekClientState.LOGGING_IN,
                        loggedIn()),
                states);
        assertEquals(List.of("wait", "raw", "message", "message", "message", "distributed"), fixture.sequence);
        fixture.connection.tokens.forEach(observed -> assertSame(token, observed));
        assertSame(token, fixture.waiter.token);
        fixture.close();
    }

    @Test
    void adoptsOurOwnStatisticsAsTheAdvertisedUploadSpeed() {
        Fixture fixture = new Fixture();
        fixture.connection.fireConnected = true;
        fixture.client.connect("127.0.0.1", 2271, "alice", "secret", CancellationSignal.none());

        fixture.factory.messageRead.accept(new MessageEvent(fixture.connection.proxy, statistics("bob", 999_999)));
        assertEquals(
                0,
                fixture.client.transfers().advertisedUploadSpeed(),
                "another user's statistics are not ours to advertise");

        fixture.factory.messageRead.accept(new MessageEvent(fixture.connection.proxy, statistics("alice", 52_000)));
        assertEquals(
                52_000,
                fixture.client.transfers().advertisedUploadSpeed(),
                "our own statistics carry the server's upload average, which is what peers are shown");
        fixture.close();
    }

    /** A statistics response as the server encodes one. */
    private static byte[] statistics(String username, int averageSpeed) {
        return new dev.slsk.internal.messaging.MessageBuilder()
                .writeCode(MessageCode.Server.GET_USER_STATS)
                .writeString(username)
                .writeInteger(averageSpeed)
                .writeLong(3)
                .writeInteger(2)
                .writeInteger(1)
                .build();
    }

    @Test
    void raisesAndStoresSupporterServerInfoBeforeLoggedInState() {
        Fixture fixture = new Fixture();
        fixture.waiter.response = new LoginResponse(true, "", null, null, true);
        List<String> sequence = new ArrayList<>();
        fixture.client
                .events()
                .on(Kind.SERVER_INFO_RECEIVED, (dev.slsk.internal.connection.ServerSessionInfo eventData) -> {
                    assertTrue(eventData.supporter());
                    assertNull(fixture.client.getUsername());
                    sequence.add("server-info");
                });
        fixture.client
                .events()
                .on(Kind.STATE_CHANGED, (dev.slsk.internal.events.SoulseekClientStateChangedEvent eventData) -> {
                    if (eventData.state().equals(loggedIn())) {
                        sequence.add("logged-in");
                    }
                });

        fixture.client.connect("127.0.0.1", 2271, "alice", "secret");

        assertEquals(List.of("server-info", "logged-in"), sequence);
        assertTrue(fixture.client.getServerInfo().supporter());
        fixture.close();
    }

    @Test
    void rejectedLoginIsPreservedAndDisconnects() {
        Fixture fixture = new Fixture();
        fixture.waiter.response = new LoginResponse(false, "denied");

        LoginRejectedException failure = assertInstanceOf(
                LoginRejectedException.class,
                completionCause(() -> fixture.client.connect("127.0.0.1", 2271, "alice", "secret")));

        assertTrue(failure.getMessage().contains("denied"));
        assertEquals(SoulseekClientState.DISCONNECTED, fixture.client.getState());
        assertNull(fixture.client.getUsername());
        assertEquals(1, fixture.connection.disconnectCount);
        fixture.close();
    }

    @Test
    void wrapsConnectionAndConfigurationFailuresAndDisconnects() {
        Fixture fixture = new Fixture();
        ConnectionException connectFailure = new ConnectionException("connect");
        fixture.connection.synchronousConnectFailure = connectFailure;
        SoulseekClientException wrapped = assertInstanceOf(
                SoulseekClientException.class,
                completionCause(() -> fixture.client.connect("127.0.0.1", 2271, "alice", "secret")));
        assertSame(connectFailure, wrapped.getCause());
        assertEquals(SoulseekClientState.DISCONNECTED, fixture.client.getState());

        Fixture configurationFixture = new Fixture();
        ConnectionWriteException writeFailure = new ConnectionWriteException("write");
        configurationFixture.connection.messageResult = CompletableFuture.failedFuture(writeFailure);
        SoulseekClientException configurationWrapped = assertInstanceOf(
                SoulseekClientException.class,
                completionCause(() -> configurationFixture.client.connect("127.0.0.1", 2271, "alice", "secret")));
        assertSame(writeFailure, configurationWrapped.getCause());
        assertEquals(SoulseekClientState.DISCONNECTED, configurationFixture.client.getState());
        fixture.close();
        configurationFixture.close();
    }

    @Test
    void preservesConnectionTimeoutAndCallerCancellation() {
        Fixture fixture = new Fixture();
        TimeoutException timeout = new TimeoutException("timeout");
        fixture.connection.connectResult = CompletableFuture.failedFuture(timeout);
        assertSame(
                timeout,
                assertInstanceOf(
                                NoResponseException.class,
                                completionCause(() -> fixture.client.connect("127.0.0.1", 2271, "alice", "secret")))
                        .getCause());
        assertEquals(SoulseekClientState.DISCONNECTED, fixture.client.getState());

        Fixture cancelledFixture = new Fixture();
        CancellationController source = new CancellationController();
        source.cancel();
        assertInstanceOf(
                CancellationException.class,
                completionCause(() ->
                        cancelledFixture.client.connect("127.0.0.1", 2271, "alice", "secret", source.getSignal())));
        assertEquals(0, cancelledFixture.connection.connectCount);
        assertEquals(SoulseekClientState.DISCONNECTED, cancelledFixture.client.getState());
        fixture.close();
        cancelledFixture.close();
    }

    @Test
    void serverConnectionCallbacksDriveStateAndMessageHandlers() {
        Fixture fixture = new Fixture();
        fixture.client.connect("127.0.0.1", 2271, "alice", "secret");
        assertTrue(fixture.factory.connected != null);
        assertTrue(fixture.factory.disconnected != null);
        assertTrue(fixture.factory.messageRead != null);
        assertTrue(fixture.factory.messageWritten != null);

        fixture.factory.connected.accept(fixture.connection.proxy);
        assertEquals(SoulseekClientState.CONNECTED, fixture.client.getState());
        fixture.factory.disconnected.accept(new ConnectionDisconnectedEvent(fixture.connection.proxy, "gone", null));
        assertEquals(SoulseekClientState.DISCONNECTED, fixture.client.getState());
        fixture.close();
    }

    private static SoulseekClientState loggedIn() {
        return SoulseekClientState.LOGGED_IN;
    }

    /**
     * Returns the failure a blocking call produced.
     *
     * <p>Took a future before the API became blocking; the calls now throw
     * directly, so it takes the call itself.
     */
    private static Throwable completionCause(org.junit.jupiter.api.function.Executable body) {
        try {
            body.execute();
        } catch (java.util.concurrent.CompletionException wrapped) {
            return wrapped.getCause() == null ? wrapped : wrapped.getCause();
        } catch (Throwable failure) {
            return failure;
        }
        throw new AssertionError("expected the operation to fail");
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
            return 0D;
        }
        return null;
    }

    private static final class Fixture {
        private final List<String> sequence = new ArrayList<>();
        private final SoulseekClientOptions options;
        private final ConnectionProbe connection = new ConnectionProbe(sequence);
        private final ConnectionFactoryProbe factory = new ConnectionFactoryProbe(connection);
        private final WaiterProbe waiter = new WaiterProbe(sequence);
        private final DistributedProbe distributed = new DistributedProbe(sequence);
        private final SoulseekEngine client;

        private Fixture() {
            this(SoulseekClientOptions.builder().enableListener(false).build());
        }

        private Fixture(SoulseekClientOptions clientOptions) {
            options = clientOptions;
            client = new SoulseekEngine(
                    9999,
                    options,
                    null,
                    factory.proxy,
                    null,
                    distributed.proxy,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    waiter.proxy,
                    null,
                    null,
                    null,
                    null,
                    null);
        }

        private void close() {
            client.close();
        }
    }

    private static final class ConnectionProbe {
        private final List<String> sequence;
        private final List<byte[]> rawMessages = new ArrayList<>();
        private final List<OutgoingMessage> outgoingMessages = new ArrayList<>();
        private final List<CancellationSignal> tokens = new ArrayList<>();
        private volatile CompletableFuture<Void> connectResult = CompletableFuture.completedFuture(null);
        private volatile CompletableFuture<Void> rawResult = CompletableFuture.completedFuture(null);
        private volatile CompletableFuture<Void> messageResult = CompletableFuture.completedFuture(null);
        private RuntimeException synchronousConnectFailure;
        private ConnectionFactoryProbe factory;
        private CancellationSignal connectToken;
        private int connectCount;
        private int disconnectCount;
        private boolean fireConnected;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private ConnectionProbe(List<String> sequence) {
            this.sequence = sequence;
        }

        private Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            if (method.getName().equals("connect")) {
                connectCount++;
                connectToken = (CancellationSignal) arguments[0];
                if (synchronousConnectFailure != null) {
                    throw synchronousConnectFailure;
                }
                if (fireConnected && factory.connected != null) {
                    factory.connected.accept(proxy);
                }
                // The configured outcome is raised as itself, which is what
                // the blocking transport raises now.
                Outcomes.raise(connectResult);
                return null;
            }
            if (method.getName().equals("write") && arguments.length == 2 && arguments[0] instanceof byte[] bytes) {
                sequence.add("raw");
                rawMessages.add(bytes);
                tokens.add((CancellationSignal) arguments[1]);
                Outcomes.raise(rawResult);
                return null;
            }
            if (method.getName().equals("write")
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage message) {
                sequence.add("message");
                outgoingMessages.add(message);
                tokens.add((CancellationSignal) arguments[1]);
                Outcomes.raise(messageResult);
                return null;
            }
            if (method.getName().equals("disconnect")) {
                disconnectCount++;
                return null;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class ConnectionFactoryProbe {
        private final MessageConnection connection;
        private InetSocketAddress endpoint;
        private dev.slsk.internal.options.ConnectionOptions options;
        private java.util.function.Consumer<TransportConnection> connected;
        private java.util.function.Consumer<ConnectionDisconnectedEvent> disconnected;
        private java.util.function.Consumer<MessageEvent> messageRead;
        private java.util.function.Consumer<MessageEvent> messageWritten;
        private final ConnectionFactory proxy = (ConnectionFactory) Proxy.newProxyInstance(
                ConnectionFactory.class.getClassLoader(), new Class<?>[] {ConnectionFactory.class}, this::invoke);

        private ConnectionFactoryProbe(ConnectionProbe connectionProbe) {
            connection = connectionProbe.proxy;
            connectionProbe.factory = this;
        }

        @SuppressWarnings("unchecked")
        private Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            if (method.getName().equals("getServerConnection")) {
                endpoint = (InetSocketAddress) arguments[0];
                connected = (java.util.function.Consumer<TransportConnection>) arguments[1];
                disconnected = (java.util.function.Consumer<ConnectionDisconnectedEvent>) arguments[2];
                messageRead = (java.util.function.Consumer<MessageEvent>) arguments[3];
                messageWritten = (java.util.function.Consumer<MessageEvent>) arguments[4];
                options = (dev.slsk.internal.options.ConnectionOptions) arguments[5];
                return connection;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class WaiterProbe {
        private final List<String> sequence;
        private LoginResponse response = new LoginResponse(true, "");
        private CancellationSignal token;
        private final Waiter proxy = (Waiter)
                Proxy.newProxyInstance(Waiter.class.getClassLoader(), new Class<?>[] {Waiter.class}, this::invoke);

        private WaiterProbe(List<String> sequence) {
            this.sequence = sequence;
        }

        private Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            if (method.getName().equals("register") && arguments.length == 4) {
                assertEquals(new WaitKey.ServerMessage(MessageCode.Server.LOGIN), arguments[0]);
                assertSame(LoginResponse.class, arguments[1]);
                assertEquals(Duration.ofSeconds(5), arguments[2]);
                token = (CancellationSignal) arguments[3];
                sequence.add("wait");
                return (Wait<Object>) () -> response;
            }
            if (method.getName().equals("getDefaultTimeout")) {
                return Duration.ofSeconds(5);
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class DistributedProbe {
        private final List<String> sequence;
        private int updateCount;
        private CancellationSignal updateToken;
        private final DistributedConnectionManager proxy = (DistributedConnectionManager) Proxy.newProxyInstance(
                DistributedConnectionManager.class.getClassLoader(),
                new Class<?>[] {DistributedConnectionManager.class},
                this::invoke);

        private DistributedProbe(List<String> sequence) {
            this.sequence = sequence;
        }

        private Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            if (method.getName().equals("updateStatus") && arguments != null) {
                updateCount++;
                updateToken = (CancellationSignal) arguments[0];
                sequence.add("distributed");
                return null;
            }
            return defaultValue(method.getReturnType());
        }
    }
}
