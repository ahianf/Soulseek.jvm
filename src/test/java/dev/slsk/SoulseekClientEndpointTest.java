// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.common.IWaiter;
import dev.slsk.common.WaitKey;
import dev.slsk.diagnostics.DiagnosticLevel;
import dev.slsk.exceptions.UserEndPointCacheException;
import dev.slsk.exceptions.UserEndPointException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.messages.OutgoingMessage;
import dev.slsk.messaging.messages.UserAddressRequest;
import dev.slsk.messaging.messages.UserAddressResponse;
import dev.slsk.network.MessageConnection;
import dev.slsk.options.SoulseekClientOptions;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class SoulseekClientEndpointTest {
    private static final InetSocketAddress ENDPOINT = new InetSocketAddress(InetAddress.getLoopbackAddress(), 46001);

    @Test
    void validatesArgumentsAndLoginState() {
        Fixture fixture = new Fixture(null);
        for (String bad : new String[] {null, "", " ", "\t"}) {
            assertThrows(IllegalArgumentException.class, () -> fixture.client.getUserEndPointAsync(bad));
        }
        fixture.client.setStateForTest(SoulseekClientStates.DISCONNECTED);
        assertThrows(IllegalStateException.class, () -> fixture.client.getUserEndPointAsync("alice"));
        fixture.close();
    }

    @Test
    void returnsExpectedEndpointUsingExactCorrelationAndToken() {
        Fixture fixture = new Fixture(null);
        fixture.waiter.result = CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT));
        CancellationTokenSource source = new CancellationTokenSource();
        CancellationToken token = source.getToken();

        InetSocketAddress actual =
                fixture.client.getUserEndPointAsync("alice", token).join();

        assertEquals(ENDPOINT, actual);
        assertEquals(new WaitKey(MessageCode.Server.GET_PEER_ADDRESS, "alice"), fixture.waiter.key);
        assertSame(UserAddressResponse.class, fixture.waiter.resultType);
        assertSame(token, fixture.waiter.token);
        assertSame(token, fixture.connection.token);
        assertEquals(
                "alice",
                assertInstanceOf(UserAddressRequest.class, fixture.connection.message)
                        .getUsername());
        fixture.close();
    }

    @Test
    void preservesOfflineTimeoutCancellationAndMapsOtherFailures() {
        Fixture fixture = new Fixture(null);
        UserOfflineException offline = new UserOfflineException("offline");
        fixture.waiter.result = CompletableFuture.completedFuture(
                new UserAddressResponse("alice", new InetSocketAddress("0.0.0.0", 0)));
        assertInstanceOf(UserOfflineException.class, failureOf(fixture.client.getUserEndPointAsync("alice")));

        TimeoutException timeout = new TimeoutException("timed out");
        fixture.waiter.result = CompletableFuture.failedFuture(timeout);
        assertSame(timeout, failureOf(fixture.client.getUserEndPointAsync("bob")));

        CancellationException cancellation = new CancellationException("cancelled");
        fixture.waiter.result = CompletableFuture.failedFuture(cancellation);
        assertSame(cancellation, failureOf(fixture.client.getUserEndPointAsync("carol")));

        RuntimeException expected = new RuntimeException("wait failed");
        fixture.waiter.synchronousFailure = expected;
        UserEndPointException mapped =
                assertInstanceOf(UserEndPointException.class, failureOf(fixture.client.getUserEndPointAsync("dave")));
        assertSame(expected, mapped.getCause());
        fixture.close();
    }

    @Test
    void cacheHitReturnsWithoutNetworkAndMissUpdatesCache() {
        CacheProbe cache = new CacheProbe();
        cache.value = CacheLookupResult.found(ENDPOINT);
        Fixture fixture = new Fixture(cache);

        assertEquals(ENDPOINT, fixture.client.getUserEndPointAsync("alice").join());
        assertEquals(0, fixture.connection.writes);
        assertEquals(0, fixture.waiter.registrations);

        cache.value = CacheLookupResult.notFound();
        InetSocketAddress second = new InetSocketAddress(InetAddress.getLoopbackAddress(), 46002);
        fixture.waiter.result = CompletableFuture.completedFuture(new UserAddressResponse("bob", second));
        assertEquals(second, fixture.client.getUserEndPointAsync("bob").join());
        assertEquals("bob", cache.updatedUsername);
        assertEquals(second, cache.updatedEndpoint);
        fixture.close();
    }

    @Test
    void wrapsBothCacheReadAndUpdateFailures() {
        CacheProbe readCache = new CacheProbe();
        RuntimeException readFailure = new RuntimeException("read failed");
        readCache.readFailure = readFailure;
        Fixture readFixture = new Fixture(readCache);
        UserEndPointCacheException readMapped =
                assertThrows(UserEndPointCacheException.class, () -> readFixture.client.getUserEndPointAsync("alice"));
        assertSame(readFailure, readMapped.getCause());
        readFixture.close();

        CacheProbe updateCache = new CacheProbe();
        updateCache.value = CacheLookupResult.notFound();
        RuntimeException updateFailure = new RuntimeException("update failed");
        updateCache.updateFailure = updateFailure;
        Fixture updateFixture = new Fixture(updateCache);
        updateFixture.waiter.result = CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT));
        UserEndPointCacheException updateMapped = assertInstanceOf(
                UserEndPointCacheException.class, failureOf(updateFixture.client.getUserEndPointAsync("alice")));
        assertSame(updateFailure, updateMapped.getCause());
        updateFixture.close();
    }

    @Test
    void coalescesConcurrentSameUserRequestsAndCleansUpAfterward() {
        Fixture fixture = new Fixture(null);
        CompletableFuture<UserAddressResponse> response = new CompletableFuture<>();
        fixture.waiter.result = response;

        CompletableFuture<InetSocketAddress> first = fixture.client.getUserEndPointAsync("alice");
        CompletableFuture<InetSocketAddress> second = fixture.client.getUserEndPointAsync("alice");

        assertFalse(first.isDone());
        assertFalse(second.isDone());
        assertEquals(1, fixture.connection.writes);
        assertEquals(1, fixture.waiter.registrations);
        response.complete(new UserAddressResponse("alice", ENDPOINT));
        assertEquals(ENDPOINT, first.join());
        assertEquals(ENDPOINT, second.join());

        fixture.waiter.result = CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT));
        fixture.client.getUserEndPointAsync("alice").join();
        assertEquals(2, fixture.connection.writes);
        assertEquals(2, fixture.waiter.registrations);
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

    private static SoulseekClientOptions options(IUserEndPointCache cache) {
        return new SoulseekClientOptions(
                false,
                InetAddress.getLoopbackAddress(),
                50_000,
                true,
                true,
                25,
                50,
                1,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                true,
                5_000,
                true,
                true,
                false,
                DiagnosticLevel.NONE,
                0,
                null,
                null,
                null,
                null,
                null,
                cache,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                false);
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
        private final ConnectionProbe connection = new ConnectionProbe();
        private final WaiterProbe waiter = new WaiterProbe();
        private final SoulseekClient client;

        private Fixture(IUserEndPointCache cache) {
            client = new SoulseekClient(
                    9999,
                    options(cache),
                    connection.proxy,
                    null,
                    null,
                    null,
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
            client.setStateForTest(SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGED_IN));
        }

        @Override
        public void close() {
            client.close();
        }
    }

    private static final class ConnectionProbe {
        private OutgoingMessage message;
        private CancellationToken token;
        private int writes;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("writeAsync")
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage outgoing) {
                writes++;
                message = outgoing;
                token = (CancellationToken) arguments[1];
                return CompletableFuture.completedFuture(null);
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class WaiterProbe {
        private WaitKey key;
        private Class<?> resultType;
        private CancellationToken token;
        private int registrations;
        private CompletableFuture<UserAddressResponse> result = new CompletableFuture<>();
        private Throwable synchronousFailure;
        private final IWaiter proxy = (IWaiter)
                Proxy.newProxyInstance(IWaiter.class.getClassLoader(), new Class<?>[] {IWaiter.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) throws Throwable {
            if (method.getName().equals("waitAsync") && arguments.length == 4) {
                registrations++;
                key = (WaitKey) arguments[0];
                resultType = (Class<?>) arguments[1];
                token = (CancellationToken) arguments[3];
                if (synchronousFailure != null) {
                    throw synchronousFailure;
                }
                return result;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class CacheProbe implements IUserEndPointCache {
        private CacheLookupResult<InetSocketAddress> value = CacheLookupResult.notFound();
        private RuntimeException readFailure;
        private RuntimeException updateFailure;
        private String updatedUsername;
        private InetSocketAddress updatedEndpoint;

        @Override
        public CacheLookupResult<InetSocketAddress> tryGet(String username) {
            if (readFailure != null) {
                throw readFailure;
            }
            return value;
        }

        @Override
        public void addOrUpdate(String username, InetSocketAddress endPoint) {
            if (updateFailure != null) {
                throw updateFailure;
            }
            updatedUsername = username;
            updatedEndpoint = endPoint;
            value = CacheLookupResult.found(endPoint);
        }
    }
}
