// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.common.IWaiter;
import dev.slsk.common.WaitKey;
import dev.slsk.diagnostics.IDiagnosticFactory;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.UserEndPointException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.messages.IOutgoingMessage;
import dev.slsk.messaging.messages.UserAddressRequest;
import dev.slsk.messaging.messages.UserAddressResponse;
import dev.slsk.messaging.messages.UserInfoRequest;
import dev.slsk.network.IMessageConnection;
import dev.slsk.network.IPeerConnectionManager;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class SoulseekClientPeerRequestTest {
    private static final InetSocketAddress ENDPOINT = new InetSocketAddress(InetAddress.getLoopbackAddress(), 46001);

    @Test
    void connectToUserResolvesEndpointAndGetsConnection() {
        Fixture fixture = new Fixture();
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        CancellationTokenSource source = new CancellationTokenSource();
        CancellationToken token = source.getToken();

        fixture.client.connectToUserAsync("alice", token).join();

        assertEquals("alice", fixture.peerManager.username);
        assertEquals(ENDPOINT, fixture.peerManager.endpoint);
        assertSame(token, fixture.peerManager.token);
        assertEquals(0, fixture.peerManager.invalidations);
        assertEquals(
                "alice",
                assertInstanceOf(UserAddressRequest.class, fixture.server.message)
                        .getUsername());
        assertSame(token, fixture.server.token);
        fixture.close();
    }

    @Test
    void connectToUserInvalidatesOnRequestAndLogsOnlySuccess() {
        Fixture fixture = new Fixture();
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        fixture.peerManager.invalidationResult = false;

        fixture.client.connectToUserAsync("alice", true).join();

        assertEquals(1, fixture.peerManager.invalidations);
        assertEquals(List.of(), fixture.diagnostic.debugMessages);

        fixture.peerManager.invalidationResult = true;
        fixture.client.connectToUserAsync("alice", true).join();
        assertEquals(2, fixture.peerManager.invalidations);
        assertEquals(List.of("Invalidated message connection cache for alice"), fixture.diagnostic.debugMessages);
        fixture.close();
    }

    @Test
    void getUserInfoRegistersWaitThenUsesPeerConnection() {
        Fixture fixture = new Fixture();
        UserInfo expected = new UserInfo("description", 3, 4, true, new byte[] {1, 2});
        fixture.waiter.results.put(UserInfo.class, CompletableFuture.completedFuture(expected));
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        CancellationTokenSource source = new CancellationTokenSource();
        CancellationToken token = source.getToken();

        UserInfo actual = fixture.client.getUserInfoAsync("alice", token).join();

        assertSame(expected, actual);
        assertEquals(
                List.of(
                        new WaitKey(MessageCode.Peer.INFO_RESPONSE, "alice"),
                        new WaitKey(MessageCode.Server.GET_PEER_ADDRESS, "alice")),
                fixture.waiter.keys);
        assertEquals("alice", fixture.peerManager.username);
        assertEquals(ENDPOINT, fixture.peerManager.endpoint);
        assertSame(token, fixture.peerManager.token);
        assertInstanceOf(UserInfoRequest.class, fixture.peer.message);
        assertSame(token, fixture.peer.token);
        fixture.waiter.tokens.forEach(recorded -> assertSame(token, recorded));
        fixture.close();
    }

    @Test
    void validatesArgumentsAndLoginState() {
        Fixture fixture = new Fixture();
        for (String bad : new String[] {null, "", " ", "\t"}) {
            assertThrows(IllegalArgumentException.class, () -> fixture.client.connectToUserAsync(bad));
            assertThrows(IllegalArgumentException.class, () -> fixture.client.getUserInfoAsync(bad));
        }
        fixture.client.setStateForTest(SoulseekClientStates.DISCONNECTED);
        assertThrows(IllegalStateException.class, () -> fixture.client.connectToUserAsync("alice"));
        assertThrows(IllegalStateException.class, () -> fixture.client.getUserInfoAsync("alice"));
        fixture.close();
    }

    @Test
    void connectPreservesOfflineTimeoutAndCancellation() {
        Fixture fixture = new Fixture();
        UserOfflineException offline = new UserOfflineException("offline");
        fixture.waiter.results.put(UserAddressResponse.class, CompletableFuture.failedFuture(offline));
        assertSame(offline, failureOf(fixture.client.connectToUserAsync("alice")));

        TimeoutException timeout = new TimeoutException("timed out");
        fixture.server.result = CompletableFuture.failedFuture(timeout);
        fixture.waiter.results.put(UserAddressResponse.class, new CompletableFuture<>());
        assertSame(timeout, failureOf(fixture.client.connectToUserAsync("bob")));

        CancellationException cancellation = new CancellationException("cancelled");
        fixture.server.result = CompletableFuture.failedFuture(cancellation);
        assertSame(cancellation, failureOf(fixture.client.connectToUserAsync("carol")));
        fixture.close();
    }

    @Test
    void connectWrapsEndpointAndManagerFailuresAtSourceLayers() {
        Fixture fixture = new Fixture();
        RuntimeException endpointFailure = new RuntimeException("endpoint failed");
        fixture.server.synchronousFailure = endpointFailure;

        SoulseekClientException outer =
                assertInstanceOf(SoulseekClientException.class, failureOf(fixture.client.connectToUserAsync("alice")));
        UserEndPointException endpoint = assertInstanceOf(UserEndPointException.class, outer.getCause());
        assertSame(endpointFailure, endpoint.getCause());

        fixture.server.synchronousFailure = null;
        fixture.server.result = CompletableFuture.completedFuture(null);
        fixture.waiter.results.put(
                UserAddressResponse.class, CompletableFuture.completedFuture(new UserAddressResponse("bob", ENDPOINT)));
        RuntimeException managerFailure = new RuntimeException("manager failed");
        fixture.peerManager.synchronousFailure = managerFailure;

        SoulseekClientException managerMapped =
                assertInstanceOf(SoulseekClientException.class, failureOf(fixture.client.connectToUserAsync("bob")));
        assertSame(managerFailure, managerMapped.getCause());
        fixture.close();
    }

    @Test
    void getUserInfoMapsPeerFailuresAndPreservesSpecialCases() {
        Fixture fixture = new Fixture();
        UserInfo info = new UserInfo("description", 1, 2, false);
        fixture.waiter.results.put(UserInfo.class, CompletableFuture.completedFuture(info));
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));

        TimeoutException timeout = new TimeoutException("timed out");
        fixture.peer.result = CompletableFuture.failedFuture(timeout);
        assertSame(timeout, failureOf(fixture.client.getUserInfoAsync("alice")));

        CancellationException cancellation = new CancellationException("cancelled");
        fixture.peer.result = CompletableFuture.failedFuture(cancellation);
        assertSame(cancellation, failureOf(fixture.client.getUserInfoAsync("bob")));

        RuntimeException expected = new RuntimeException("peer write failed");
        fixture.peer.result = CompletableFuture.failedFuture(expected);
        SoulseekClientException mapped =
                assertInstanceOf(SoulseekClientException.class, failureOf(fixture.client.getUserInfoAsync("carol")));
        assertSame(expected, mapped.getCause());

        UserOfflineException offline = new UserOfflineException("offline");
        fixture.peer.result = CompletableFuture.completedFuture(null);
        fixture.waiter.results.put(UserAddressResponse.class, CompletableFuture.failedFuture(offline));
        assertSame(offline, failureOf(fixture.client.getUserInfoAsync("dave")));
        fixture.close();
    }

    private static Throwable failureOf(CompletableFuture<?> future) {
        try {
            future.join();
            throw new AssertionError("Expected operation to fail");
        } catch (CompletionException exception) {
            return exception.getCause();
        } catch (CancellationException exception) {
            return exception;
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

    private static final class Fixture implements AutoCloseable {
        private final ConnectionProbe server = new ConnectionProbe();
        private final ConnectionProbe peer = new ConnectionProbe();
        private final WaiterProbe waiter = new WaiterProbe();
        private final PeerManagerProbe peerManager = new PeerManagerProbe(peer.proxy);
        private final DiagnosticProbe diagnostic = new DiagnosticProbe();
        private final SoulseekClient client = new SoulseekClient(
                9999,
                null,
                server.proxy,
                null,
                peerManager.proxy,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                waiter.proxy,
                null,
                diagnostic.proxy,
                null,
                null,
                null);

        private Fixture() {
            client.setStateForTest(SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGED_IN));
        }

        @Override
        public void close() {
            client.close();
        }
    }

    private static final class ConnectionProbe {
        private IOutgoingMessage message;
        private CancellationToken token;
        private CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        private RuntimeException synchronousFailure;
        private final IMessageConnection proxy = (IMessageConnection) Proxy.newProxyInstance(
                IMessageConnection.class.getClassLoader(), new Class<?>[] {IMessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("writeAsync")
                    && arguments.length == 2
                    && arguments[0] instanceof IOutgoingMessage outgoing) {
                message = outgoing;
                token = (CancellationToken) arguments[1];
                if (synchronousFailure != null) {
                    throw synchronousFailure;
                }
                return result;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class WaiterProbe {
        private final Map<Class<?>, CompletableFuture<?>> results = new HashMap<>();
        private final List<WaitKey> keys = new ArrayList<>();
        private final List<CancellationToken> tokens = new ArrayList<>();
        private final IWaiter proxy = (IWaiter)
                Proxy.newProxyInstance(IWaiter.class.getClassLoader(), new Class<?>[] {IWaiter.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("waitAsync") && arguments.length == 4) {
                keys.add((WaitKey) arguments[0]);
                Class<?> resultType = (Class<?>) arguments[1];
                tokens.add((CancellationToken) arguments[3]);
                return results.getOrDefault(resultType, new CompletableFuture<>());
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class PeerManagerProbe {
        private final IMessageConnection connection;
        private String username;
        private InetSocketAddress endpoint;
        private CancellationToken token;
        private int invalidations;
        private boolean invalidationResult;
        private RuntimeException synchronousFailure;
        private final IPeerConnectionManager proxy = (IPeerConnectionManager) Proxy.newProxyInstance(
                IPeerConnectionManager.class.getClassLoader(),
                new Class<?>[] {IPeerConnectionManager.class},
                this::invoke);

        private PeerManagerProbe(IMessageConnection connection) {
            this.connection = connection;
        }

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("tryInvalidateMessageConnectionCache")) {
                invalidations++;
                return invalidationResult;
            }
            if (method.getName().equals("getOrAddMessageConnectionAsync") && arguments.length == 3) {
                if (synchronousFailure != null) {
                    throw synchronousFailure;
                }
                username = (String) arguments[0];
                endpoint = (InetSocketAddress) arguments[1];
                token = (CancellationToken) arguments[2];
                return CompletableFuture.completedFuture(connection);
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class DiagnosticProbe {
        private final List<String> debugMessages = new ArrayList<>();
        private final IDiagnosticFactory proxy = (IDiagnosticFactory) Proxy.newProxyInstance(
                IDiagnosticFactory.class.getClassLoader(), new Class<?>[] {IDiagnosticFactory.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("debug") && arguments.length == 1) {
                debugMessages.add((String) arguments[0]);
            }
            return null;
        }
    }
}
