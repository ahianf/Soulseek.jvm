// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.common.WaitKey;
import dev.slsk.common.Waiter;
import dev.slsk.exceptions.AddressException;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.ConnectionWriteException;
import dev.slsk.exceptions.ListenException;
import dev.slsk.exceptions.LoginRejectedException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.messages.LoginRequest;
import dev.slsk.messaging.messages.LoginResponse;
import dev.slsk.messaging.messages.OutgoingMessage;
import dev.slsk.messaging.messages.PrivateRoomToggle;
import dev.slsk.messaging.messages.SetListenPortCommand;
import dev.slsk.network.ConnectionFactory;
import dev.slsk.network.DistributedConnectionManager;
import dev.slsk.network.MessageConnection;
import dev.slsk.network.MessageConnectionEventListener;
import dev.slsk.network.MessageEventArgs;
import dev.slsk.network.tcp.ConnectionDisconnectedEventArgs;
import dev.slsk.network.tcp.ConnectionEventListener;
import dev.slsk.options.SoulseekClientOptions;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class SoulseekClientConnectTest {
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
            assertThrows(
                    IllegalArgumentException.class, () -> fixture.client.connectAsync(credentials[0], credentials[1]));
        }
        for (String invalid : new String[] {null, "", " ", "\t"}) {
            assertThrows(
                    IllegalArgumentException.class, () -> fixture.client.connectAsync(invalid, 1, "user", "password"));
        }
        assertThrows(
                IllegalArgumentException.class, () -> fixture.client.connectAsync("127.0.0.1", -1, "user", "password"));
        assertThrows(
                IllegalArgumentException.class,
                () -> fixture.client.connectAsync("127.0.0.1", 65_536, "user", "password"));

        fixture.client.setStateForTest(SoulseekClientStates.CONNECTING);
        assertThrows(
                IllegalStateException.class, () -> fixture.client.connectAsync("127.0.0.1", 1, "user", "password"));
        fixture.client.setStateForTest(SoulseekClientStates.LOGGING_IN);
        assertThrows(
                IllegalStateException.class, () -> fixture.client.connectAsync("127.0.0.1", 1, "user", "password"));
        fixture.client.setStateForTest(SoulseekClientStates.CONNECTED);
        assertThrows(
                IllegalStateException.class, () -> fixture.client.connectAsync("127.0.0.1", 1, "user", "password"));
        fixture.client.setStateForTest(SoulseekClientStates.DISCONNECTED);
        fixture.close();
    }

    @Test
    void wrapsAddressResolutionFailure() {
        Fixture fixture = new Fixture();
        AddressException failure = assertThrows(
                AddressException.class,
                () -> fixture.client.connectAsync("not-a-host.invalid", 2271, "user", "password"));
        assertInstanceOf(java.net.UnknownHostException.class, failure.getCause());
        fixture.close();
    }

    @Test
    void listenerPreflightFailureIsReportedBeforeConnecting() throws Exception {
        InetAddress nonLocalAddress = InetAddress.getByAddress(new byte[] {(byte) 192, 0, 2, 1});
        SoulseekClientOptions options = new SoulseekClientOptions(true, nonLocalAddress, 50_000);
        Fixture fixture = new Fixture(options);
        assertThrows(ListenException.class, () -> fixture.client.connectAsync("127.0.0.1", 2271, "user", "password"));
        assertEquals(0, fixture.connection.connectCount);
        fixture.close();
    }

    @Test
    void connectsLogsInAndSendsConfigurationInOrder() {
        Fixture fixture = new Fixture();
        CancellationTokenSource source = new CancellationTokenSource();
        CancellationToken token = source.getToken();
        List<SoulseekClientStates> states = new ArrayList<>();
        fixture.client.addStateChangedListener((sender, eventArgs) -> states.add(eventArgs.getState()));
        fixture.connection.fireConnected = true;

        fixture.client.connectAsync("127.0.0.1", 2271, "alice", "secret", token).join();

        assertEquals(1, fixture.connection.connectCount);
        assertSame(token, fixture.connection.connectToken);
        assertEquals(new InetSocketAddress(LOOPBACK, 2271), fixture.factory.endpoint);
        assertSame(fixture.options.getServerConnectionOptions(), fixture.factory.options);
        assertEquals("127.0.0.1", fixture.client.getAddress());
        assertEquals(new InetSocketAddress(LOOPBACK, 2271), fixture.client.getIpEndPoint());
        assertEquals("alice", fixture.client.getUsername());
        assertEquals(loggedIn(), fixture.client.getState());

        ByteArrayOutputStream expectedLogin = new ByteArrayOutputStream();
        expectedLogin.writeBytes(new LoginRequest(9999, "alice", "secret").toByteArray());
        expectedLogin.writeBytes(new SetListenPortCommand(fixture.options.getListenPort()).toByteArray());
        assertArrayEquals(expectedLogin.toByteArray(), fixture.connection.rawMessages.get(0));
        assertInstanceOf(SetListenPortCommand.class, fixture.connection.outgoingMessages.get(0));
        PrivateRoomToggle toggle =
                assertInstanceOf(PrivateRoomToggle.class, fixture.connection.outgoingMessages.get(1));
        assertEquals(fixture.options.isAcceptPrivateRoomInvitations(), toggle.isAcceptInvitations());
        assertEquals(1, fixture.distributed.updateCount);
        assertSame(token, fixture.distributed.updateToken);
        assertEquals(
                List.of(
                        SoulseekClientStates.CONNECTING,
                        SoulseekClientStates.CONNECTED,
                        SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGING_IN),
                        loggedIn()),
                states);
        assertEquals(List.of("wait", "raw", "message", "message", "distributed"), fixture.sequence);
        fixture.connection.tokens.forEach(observed -> assertSame(token, observed));
        assertSame(token, fixture.waiter.token);
        fixture.close();
    }

    @Test
    void raisesAndStoresSupporterServerInfoBeforeLoggedInState() {
        Fixture fixture = new Fixture();
        fixture.waiter.response = new LoginResponse(true, "", null, null, true);
        List<String> sequence = new ArrayList<>();
        fixture.client.addServerInfoReceivedListener((sender, eventArgs) -> {
            assertSame(fixture.client, sender);
            assertTrue(eventArgs.isSupporter());
            assertNull(fixture.client.getUsername());
            sequence.add("server-info");
        });
        fixture.client.addStateChangedListener((sender, eventArgs) -> {
            if (eventArgs.getState().equals(loggedIn())) {
                sequence.add("logged-in");
            }
        });

        fixture.client.connectAsync("127.0.0.1", 2271, "alice", "secret").join();

        assertEquals(List.of("server-info", "logged-in"), sequence);
        assertTrue(fixture.client.getServerInfo().isSupporter());
        fixture.close();
    }

    @Test
    void rejectedLoginIsPreservedAndDisconnects() {
        Fixture fixture = new Fixture();
        fixture.waiter.response = new LoginResponse(false, "denied");

        CompletableFuture<Void> operation = fixture.client.connectAsync("127.0.0.1", 2271, "alice", "secret");
        LoginRejectedException failure = assertInstanceOf(LoginRejectedException.class, completionCause(operation));

        assertTrue(failure.getMessage().contains("denied"));
        assertEquals(SoulseekClientStates.DISCONNECTED, fixture.client.getState());
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
                completionCause(fixture.client.connectAsync("127.0.0.1", 2271, "alice", "secret")));
        assertSame(connectFailure, wrapped.getCause());
        assertEquals(SoulseekClientStates.DISCONNECTED, fixture.client.getState());

        Fixture configurationFixture = new Fixture();
        ConnectionWriteException writeFailure = new ConnectionWriteException("write");
        configurationFixture.connection.messageResult = CompletableFuture.failedFuture(writeFailure);
        SoulseekClientException configurationWrapped = assertInstanceOf(
                SoulseekClientException.class,
                completionCause(configurationFixture.client.connectAsync("127.0.0.1", 2271, "alice", "secret")));
        assertSame(writeFailure, configurationWrapped.getCause());
        assertEquals(SoulseekClientStates.DISCONNECTED, configurationFixture.client.getState());
        fixture.close();
        configurationFixture.close();
    }

    @Test
    void preservesConnectionTimeoutAndCallerCancellation() {
        Fixture fixture = new Fixture();
        TimeoutException timeout = new TimeoutException("timeout");
        fixture.connection.connectResult = CompletableFuture.failedFuture(timeout);
        assertSame(timeout, completionCause(fixture.client.connectAsync("127.0.0.1", 2271, "alice", "secret")));
        assertEquals(SoulseekClientStates.DISCONNECTED, fixture.client.getState());

        Fixture cancelledFixture = new Fixture();
        CancellationTokenSource source = new CancellationTokenSource();
        source.cancel();
        assertInstanceOf(
                CancellationException.class,
                completionCause(
                        cancelledFixture.client.connectAsync("127.0.0.1", 2271, "alice", "secret", source.getToken())));
        assertEquals(0, cancelledFixture.connection.connectCount);
        assertEquals(SoulseekClientStates.DISCONNECTED, cancelledFixture.client.getState());
        fixture.close();
        cancelledFixture.close();
    }

    @Test
    void serverConnectionCallbacksDriveStateAndMessageHandlers() {
        Fixture fixture = new Fixture();
        fixture.client.connectAsync("127.0.0.1", 2271, "alice", "secret").join();
        assertTrue(fixture.factory.connected != null);
        assertTrue(fixture.factory.disconnected != null);
        assertTrue(fixture.factory.messageRead != null);
        assertTrue(fixture.factory.messageWritten != null);

        fixture.factory.connected.handle(fixture.connection.proxy, null);
        assertEquals(SoulseekClientStates.CONNECTED, fixture.client.getState());
        fixture.factory.disconnected.handle(fixture.connection.proxy, new ConnectionDisconnectedEventArgs("gone"));
        assertEquals(SoulseekClientStates.DISCONNECTED, fixture.client.getState());
        fixture.close();
    }

    private static SoulseekClientStates loggedIn() {
        return SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGED_IN);
    }

    private static Throwable completionCause(CompletableFuture<?> future) {
        try {
            future.join();
            throw new AssertionError("Expected failure");
        } catch (CancellationException failure) {
            return failure;
        } catch (CompletionException failure) {
            Throwable cause = failure;
            while (cause instanceof CompletionException && cause.getCause() != null) {
                cause = cause.getCause();
            }
            return cause;
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
        private final SoulseekClient client;

        private Fixture() {
            this(new SoulseekClientOptions(false));
        }

        private Fixture(SoulseekClientOptions clientOptions) {
            options = clientOptions;
            client = new SoulseekClient(
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
        private final List<CancellationToken> tokens = new ArrayList<>();
        private volatile CompletableFuture<Void> connectResult = CompletableFuture.completedFuture(null);
        private volatile CompletableFuture<Void> rawResult = CompletableFuture.completedFuture(null);
        private volatile CompletableFuture<Void> messageResult = CompletableFuture.completedFuture(null);
        private RuntimeException synchronousConnectFailure;
        private ConnectionFactoryProbe factory;
        private CancellationToken connectToken;
        private int connectCount;
        private int disconnectCount;
        private boolean fireConnected;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private ConnectionProbe(List<String> sequence) {
            this.sequence = sequence;
        }

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("connectAsync")) {
                connectCount++;
                connectToken = (CancellationToken) arguments[0];
                if (synchronousConnectFailure != null) {
                    throw synchronousConnectFailure;
                }
                if (fireConnected && factory.connected != null) {
                    factory.connected.handle(proxy, null);
                }
                return connectResult;
            }
            if (method.getName().equals("writeAsync")
                    && arguments.length == 2
                    && arguments[0] instanceof byte[] bytes) {
                sequence.add("raw");
                rawMessages.add(bytes);
                tokens.add((CancellationToken) arguments[1]);
                return rawResult;
            }
            if (method.getName().equals("writeAsync")
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage message) {
                sequence.add("message");
                outgoingMessages.add(message);
                tokens.add((CancellationToken) arguments[1]);
                return messageResult;
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
        private dev.slsk.options.ConnectionOptions options;
        private ConnectionEventListener<Void> connected;
        private ConnectionEventListener<ConnectionDisconnectedEventArgs> disconnected;
        private MessageConnectionEventListener<MessageEventArgs> messageRead;
        private MessageConnectionEventListener<MessageEventArgs> messageWritten;
        private final ConnectionFactory proxy = (ConnectionFactory) Proxy.newProxyInstance(
                ConnectionFactory.class.getClassLoader(), new Class<?>[] {ConnectionFactory.class}, this::invoke);

        private ConnectionFactoryProbe(ConnectionProbe connectionProbe) {
            connection = connectionProbe.proxy;
            connectionProbe.factory = this;
        }

        @SuppressWarnings("unchecked")
        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("getServerConnection")) {
                endpoint = (InetSocketAddress) arguments[0];
                connected = (ConnectionEventListener<Void>) arguments[1];
                disconnected = (ConnectionEventListener<ConnectionDisconnectedEventArgs>) arguments[2];
                messageRead = (MessageConnectionEventListener<MessageEventArgs>) arguments[3];
                messageWritten = (MessageConnectionEventListener<MessageEventArgs>) arguments[4];
                options = (dev.slsk.options.ConnectionOptions) arguments[5];
                return connection;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class WaiterProbe {
        private final List<String> sequence;
        private LoginResponse response = new LoginResponse(true, "");
        private CancellationToken token;
        private final Waiter proxy = (Waiter)
                Proxy.newProxyInstance(Waiter.class.getClassLoader(), new Class<?>[] {Waiter.class}, this::invoke);

        private WaiterProbe(List<String> sequence) {
            this.sequence = sequence;
        }

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("waitAsync") && arguments.length == 4) {
                assertEquals(new WaitKey(MessageCode.Server.LOGIN), arguments[0]);
                assertSame(LoginResponse.class, arguments[1]);
                assertNull(arguments[2]);
                token = (CancellationToken) arguments[3];
                sequence.add("wait");
                return CompletableFuture.completedFuture(response);
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class DistributedProbe {
        private final List<String> sequence;
        private int updateCount;
        private CancellationToken updateToken;
        private final DistributedConnectionManager proxy = (DistributedConnectionManager) Proxy.newProxyInstance(
                DistributedConnectionManager.class.getClassLoader(),
                new Class<?>[] {DistributedConnectionManager.class},
                this::invoke);

        private DistributedProbe(List<String> sequence) {
            this.sequence = sequence;
        }

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("updateStatusAsync")) {
                updateCount++;
                updateToken = (CancellationToken) arguments[0];
                sequence.add("distributed");
                return CompletableFuture.completedFuture(null);
            }
            return defaultValue(method.getReturnType());
        }
    }
}
