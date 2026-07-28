// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.CancellationController;
import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.UserEndpointCacheException;
import dev.slsk.exceptions.UserEndpointException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.diagnostics.DiagnosticLevel;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.messaging.messages.UserAddressRequest;
import dev.slsk.internal.messaging.messages.UserAddressResponse;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.options.SoulseekClientOptions;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class SoulseekClientEndpointTest {
    private static final InetSocketAddress ENDPOINT = new InetSocketAddress(InetAddress.getLoopbackAddress(), 46001);

    @Test
    void validatesArgumentsAndLoginState() {
        Fixture fixture = new Fixture(null);
        for (String bad : new String[] {null, "", " ", "\t"}) {
            assertThrows(IllegalArgumentException.class, () -> fixture.client.getUserEndpoint(bad));
        }
        fixture.client.setStateForTest(SoulseekClientState.DISCONNECTED);
        assertThrows(IllegalStateException.class, () -> fixture.client.getUserEndpoint("alice"));
        fixture.close();
    }

    @Test
    void returnsExpectedEndpointUsingExactCorrelationAndToken() {
        Fixture fixture = new Fixture(null);
        fixture.waiter.result = CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT));
        CancellationController source = new CancellationController();
        CancellationSignal token = source.getSignal();

        InetSocketAddress actual = fixture.client.getUserEndpoint("alice", token);

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
        assertInstanceOf(UserOfflineException.class, failureOf(() -> fixture.client.getUserEndpoint("alice")));

        TimeoutException timeout = new TimeoutException("timed out");
        fixture.waiter.result = CompletableFuture.failedFuture(timeout);
        assertSame(
                timeout,
                assertInstanceOf(NoResponseException.class, failureOf(() -> fixture.client.getUserEndpoint("bob")))
                        .getCause());

        CancellationException cancellation = new CancellationException("cancelled");
        fixture.waiter.result = CompletableFuture.failedFuture(cancellation);
        assertSame(cancellation, failureOf(() -> fixture.client.getUserEndpoint("carol")));

        RuntimeException expected = new RuntimeException("wait failed");
        fixture.waiter.synchronousFailure = expected;
        UserEndpointException mapped =
                assertInstanceOf(UserEndpointException.class, failureOf(() -> fixture.client.getUserEndpoint("dave")));
        assertSame(expected, mapped.getCause());
        fixture.close();
    }

    @Test
    void cacheHitReturnsWithoutNetworkAndMissUpdatesCache() {
        CacheProbe cache = new CacheProbe();
        cache.value = CacheLookupResult.found(ENDPOINT);
        Fixture fixture = new Fixture(cache);

        assertEquals(ENDPOINT, fixture.client.getUserEndpoint("alice"));
        assertEquals(0, fixture.connection.writes);
        assertEquals(0, fixture.waiter.registrations);

        cache.value = CacheLookupResult.notFound();
        InetSocketAddress second = new InetSocketAddress(InetAddress.getLoopbackAddress(), 46002);
        fixture.waiter.result = CompletableFuture.completedFuture(new UserAddressResponse("bob", second));
        assertEquals(second, fixture.client.getUserEndpoint("bob"));
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
        UserEndpointCacheException readMapped =
                assertThrows(UserEndpointCacheException.class, () -> readFixture.client.getUserEndpoint("alice"));
        assertSame(readFailure, readMapped.getCause());
        readFixture.close();

        CacheProbe updateCache = new CacheProbe();
        updateCache.value = CacheLookupResult.notFound();
        RuntimeException updateFailure = new RuntimeException("update failed");
        updateCache.updateFailure = updateFailure;
        Fixture updateFixture = new Fixture(updateCache);
        updateFixture.waiter.result = CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT));
        UserEndpointCacheException updateMapped = assertInstanceOf(
                UserEndpointCacheException.class, failureOf(() -> updateFixture.client.getUserEndpoint("alice")));
        assertSame(updateFailure, updateMapped.getCause());
        updateFixture.close();
    }

    @Test
    void issuesAnIndependentRequestPerCallerWhenNoCacheIsConfigured() {
        // The source only serializes same-user lookups when a cache is configured; with no cache
        // every caller performs its own request under its own cancellation signal.
        Fixture fixture = new Fixture(null);
        CompletableFuture<UserAddressResponse> response = new CompletableFuture<>();
        fixture.waiter.result = response;

        // Blocking calls, so the two callers need threads of their own to be
        // in flight at the same time. What is being asserted is unchanged: two
        // independent requests, two registrations, both satisfied by one
        // response.
        java.util.concurrent.atomic.AtomicReference<InetSocketAddress> first = new AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<InetSocketAddress> second = new AtomicReference<>();
        Thread firstCaller = Thread.ofVirtual().start(() -> first.set(fixture.client.getUserEndpoint("alice")));
        Thread secondCaller = Thread.ofVirtual().start(() -> second.set(fixture.client.getUserEndpoint("alice")));

        // Both callers run on their own threads; wait for the write as well as
        // the registration before asserting on either.
        awaitValue(() -> fixture.waiter.registrations == 2 && fixture.connection.writes == 2);
        assertEquals(2, fixture.connection.writes);
        assertEquals(2, fixture.waiter.registrations);

        response.complete(new UserAddressResponse("alice", ENDPOINT));
        join(firstCaller);
        join(secondCaller);
        assertEquals(ENDPOINT, first.get());
        assertEquals(ENDPOINT, second.get());
        fixture.close();
    }

    @Test
    void serializesSameUserLookupsBehindTheCacheAndSweepsIdleSemaphores() {
        CacheProbe cache = new CacheProbe();
        cache.value = CacheLookupResult.notFound();
        Fixture fixture = new Fixture(cache);
        fixture.waiter.result = CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT));

        assertEquals(ENDPOINT, fixture.client.getUserEndpoint("alice"));
        assertEquals(1, fixture.connection.writes);

        // The second caller reads the value the first stored rather than repeating the request.
        cache.value = CacheLookupResult.found(ENDPOINT);
        assertEquals(ENDPOINT, fixture.client.getUserEndpoint("alice"));
        assertEquals(1, fixture.connection.writes);

        // The idle per-user semaphore is reclaimed, matching the source's periodic sweep.
        fixture.client.cleanupUserEndpointSemaphoresAsync().join();
        assertEquals(0, fixture.client.getUserEndpointSemaphoresForTest().size());
        fixture.close();
    }

    @Test
    void oneCallersCancellationDoesNotFailAnother() {
        Fixture fixture = new Fixture(null);
        CompletableFuture<UserAddressResponse> response = new CompletableFuture<>();
        fixture.waiter.result = response;
        CancellationController firstSource = new CancellationController();

        java.util.concurrent.atomic.AtomicReference<InetSocketAddress> second = new AtomicReference<>();
        Thread firstCaller = Thread.ofVirtual().start(() -> {
            try {
                fixture.client.getUserEndpoint("alice", firstSource.getSignal());
            } catch (RuntimeException cancelled) {
                // Expected; this caller is the one being cancelled.
            }
        });
        Thread secondCaller = Thread.ofVirtual()
                .start(() -> second.set(fixture.client.getUserEndpoint("alice", CancellationSignal.none())));

        awaitValue(() -> fixture.waiter.registrations == 2);
        firstSource.cancel();
        response.complete(new UserAddressResponse("alice", ENDPOINT));

        join(firstCaller);
        join(secondCaller);
        assertEquals(ENDPOINT, second.get(), "an uncancelled caller must not inherit another caller's cancellation");
        fixture.close();
    }

    /** Waits for a condition the other caller thread will satisfy. */
    private static void awaitValue(java.util.function.BooleanSupplier condition) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }

    private static void join(Thread thread) {
        try {
            thread.join(java.util.concurrent.TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while awaiting a caller", interrupted);
        }
    }

    /**
     * Returns the failure a blocking call produced.
     *
     * <p>Took a future before the API became blocking; the calls now throw
     * directly, so it takes the call itself.
     */
    private static Throwable failureOf(org.junit.jupiter.api.function.Executable body) {
        try {
            body.execute();
        } catch (java.util.concurrent.CompletionException wrapped) {
            return wrapped.getCause() == null ? wrapped : wrapped.getCause();
        } catch (Throwable failure) {
            return failure;
        }
        throw new AssertionError("Expected operation to fail");
    }

    private static SoulseekClientOptions options(UserEndpointCache cache) {
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
        private final DefaultSoulseekClient client;

        private Fixture(UserEndpointCache cache) {
            client = new DefaultSoulseekClient(
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
            client.setStateForTest(SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGED_IN));
        }

        @Override
        public void close() {
            client.close();
        }
    }

    private static final class ConnectionProbe {
        private OutgoingMessage message;
        private CancellationSignal token;
        private int writes;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("writeAsync")
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage outgoing) {
                writes++;
                message = outgoing;
                token = (CancellationSignal) arguments[1];
                return CompletableFuture.completedFuture(null);
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class WaiterProbe {
        private WaitKey key;
        private Class<?> resultType;
        private CancellationSignal token;
        private int registrations;
        private CompletableFuture<UserAddressResponse> result = new CompletableFuture<>();
        private Throwable synchronousFailure;
        private final Waiter proxy = (Waiter)
                Proxy.newProxyInstance(Waiter.class.getClassLoader(), new Class<?>[] {Waiter.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) throws Throwable {
            if (method.getName().equals("waitAsync") && arguments.length == 4) {
                registrations++;
                key = (WaitKey) arguments[0];
                resultType = (Class<?>) arguments[1];
                token = (CancellationSignal) arguments[3];
                if (synchronousFailure != null) {
                    throw synchronousFailure;
                }
                // DefaultWaiter creates one PendingWait per registration and fails only that wait
                // when its own signal is cancelled. Mirror both properties so cancellation scoping
                // is exercised rather than stubbed away by a single shared future.
                CompletableFuture<UserAddressResponse> registered = new CompletableFuture<>();
                result.whenComplete((value, failure) -> {
                    if (failure != null) {
                        registered.completeExceptionally(failure);
                    } else {
                        registered.complete(value);
                    }
                });
                if (token != null) {
                    token.register(() -> registered.completeExceptionally(new CancellationException("cancelled")));
                }
                return registered;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class CacheProbe implements UserEndpointCache {
        private CacheLookupResult<InetSocketAddress> value = CacheLookupResult.notFound();
        private RuntimeException readFailure;
        private RuntimeException updateFailure;
        private String updatedUsername;
        private InetSocketAddress updatedEndpoint;

        @Override
        public CacheLookupResult<InetSocketAddress> lookup(String username) {
            if (readFailure != null) {
                throw readFailure;
            }
            return value;
        }

        @Override
        public void put(String username, InetSocketAddress endpoint) {
            if (updateFailure != null) {
                throw updateFailure;
            }
            updatedUsername = username;
            updatedEndpoint = endpoint;
            value = CacheLookupResult.found(endpoint);
        }
    }
}
