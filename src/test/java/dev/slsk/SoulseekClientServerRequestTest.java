// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.common.IWaiter;
import dev.slsk.common.WaitKey;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.messages.CheckPrivilegesRequest;
import dev.slsk.messaging.messages.GivePrivilegesCommand;
import dev.slsk.messaging.messages.NewPassword;
import dev.slsk.messaging.messages.ServerPing;
import dev.slsk.network.IMessageConnection;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class SoulseekClientServerRequestTest {
    @Test
    void changePasswordUsesTypedCorrelationAndConfirmsResponse() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (SoulseekClient client = loggedInClient(connection, waiter)) {
            CancellationTokenSource source = new CancellationTokenSource();
            CancellationToken token = source.getToken();
            waiter.result = new CompletableFuture<String>();

            CompletableFuture<Void> operation = client.changePasswordAsync("new password", token);

            assertFalse(operation.isDone());
            assertEquals(new WaitKey(MessageCode.Server.NEW_PASSWORD), waiter.key);
            assertSame(String.class, waiter.resultType);
            assertSame(token, waiter.token);
            assertSame(token, connection.token);
            assertEquals(
                    "new password",
                    assertInstanceOf(NewPassword.class, connection.message).getPassword());
            complete(waiter.result, "new password");
            operation.join();

            waiter.result = CompletableFuture.completedFuture("different");
            SoulseekClientException mismatch = assertInstanceOf(
                    SoulseekClientException.class, failureOf(client.changePasswordAsync("new password")));
            assertTrue(mismatch.getMessage().contains("doesn't match the specified password"));
        }
    }

    @Test
    void getPrivilegesReturnsTypedCorrelatedResponse() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (SoulseekClient client = loggedInClient(connection, waiter)) {
            CancellationTokenSource source = new CancellationTokenSource();
            CancellationToken token = source.getToken();
            waiter.result = CompletableFuture.completedFuture(42);

            int result = client.getPrivilegesAsync(token).join();

            assertEquals(42, result);
            assertEquals(new WaitKey(MessageCode.Server.CHECK_PRIVILEGES), waiter.key);
            assertSame(Integer.class, waiter.resultType);
            assertSame(token, waiter.token);
            assertSame(token, connection.token);
            assertInstanceOf(CheckPrivilegesRequest.class, connection.message);
        }
    }

    @Test
    void pingTimesCorrelatedRoundTrip() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (SoulseekClient client = loggedInClient(connection, waiter)) {
            CancellationTokenSource source = new CancellationTokenSource();
            CancellationToken token = source.getToken();
            waiter.result = new CompletableFuture<Void>();

            CompletableFuture<Long> operation = client.pingServerAsync(token);

            assertFalse(operation.isDone());
            assertEquals(new WaitKey(MessageCode.Server.PING), waiter.key);
            assertEquals(3, waiter.argumentCount);
            assertSame(token, waiter.token);
            assertSame(token, connection.token);
            assertInstanceOf(ServerPing.class, connection.message);
            complete(waiter.result, null);
            assertTrue(operation.join() >= 0);
        }
    }

    @Test
    void grantPrivilegesWritesWithoutWaitingForAResponse() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (SoulseekClient client = loggedInClient(connection, waiter)) {
            CancellationTokenSource source = new CancellationTokenSource();
            CancellationToken token = source.getToken();

            client.grantUserPrivilegesAsync("alice", 7, token).join();

            GivePrivilegesCommand command = assertInstanceOf(GivePrivilegesCommand.class, connection.message);
            assertEquals("alice", command.getUsername());
            assertEquals(7, command.getDays());
            assertSame(token, connection.token);
            assertEquals(0, waiter.registrations);
        }
    }

    @Test
    void validatesArgumentsAndLoginStateInSourceOrder() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (SoulseekClient client = loggedInClient(connection, waiter)) {
            for (String bad : new String[] {null, "", " ", "\t"}) {
                assertThrows(IllegalArgumentException.class, () -> client.changePasswordAsync(bad));
                assertThrows(IllegalArgumentException.class, () -> client.grantUserPrivilegesAsync(bad, 1));
            }
            assertThrows(IllegalArgumentException.class, () -> client.grantUserPrivilegesAsync("user", 0));
            assertThrows(IllegalArgumentException.class, () -> client.grantUserPrivilegesAsync("user", -1));

            client.setStateForTest(SoulseekClientStates.DISCONNECTED);
            assertThrows(IllegalStateException.class, () -> client.changePasswordAsync("password"));
            assertThrows(IllegalStateException.class, client::getPrivilegesAsync);
            assertThrows(IllegalStateException.class, client::pingServerAsync);
            assertThrows(IllegalStateException.class, () -> client.grantUserPrivilegesAsync("user", 1));
            assertThrows(IllegalArgumentException.class, () -> client.changePasswordAsync(null));
            assertThrows(IllegalArgumentException.class, () -> client.grantUserPrivilegesAsync(null, 0));
        }
    }

    @Test
    void mapsOrdinaryFailuresAndPreservesTimeoutAndCancellation() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (SoulseekClient client = loggedInClient(connection, waiter)) {
            List<Operation> operations = List.of(
                    () -> client.changePasswordAsync("password"),
                    client::getPrivilegesAsync,
                    client::pingServerAsync,
                    () -> client.grantUserPrivilegesAsync("user", 1));
            for (Operation operation : operations) {
                RuntimeException expected = new RuntimeException("write failed");
                connection.synchronousFailure = expected;
                SoulseekClientException mapped =
                        assertInstanceOf(SoulseekClientException.class, failureOf(operation.run()));
                assertSame(expected, mapped.getCause());
                connection.synchronousFailure = null;

                TimeoutException timeout = new TimeoutException("timed out");
                connection.result = CompletableFuture.failedFuture(timeout);
                assertSame(timeout, failureOf(operation.run()));

                CancellationException cancellation = new CancellationException("cancelled");
                connection.result = CompletableFuture.failedFuture(cancellation);
                assertSame(cancellation, failureOf(operation.run()));
                connection.result = CompletableFuture.completedFuture(null);
            }
        }
    }

    @Test
    void typedAndUntypedWaitFailuresUseSourceMapping() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (SoulseekClient client = loggedInClient(connection, waiter)) {
            List<Operation> operations = List.of(
                    () -> client.changePasswordAsync("password"), client::getPrivilegesAsync, client::pingServerAsync);
            for (Operation operation : operations) {
                RuntimeException expected = new RuntimeException("wait failed");
                waiter.result = CompletableFuture.failedFuture(expected);
                SoulseekClientException mapped =
                        assertInstanceOf(SoulseekClientException.class, failureOf(operation.run()));
                assertSame(expected, mapped.getCause());

                TimeoutException timeout = new TimeoutException("timed out");
                waiter.result = CompletableFuture.failedFuture(timeout);
                assertSame(timeout, failureOf(operation.run()));

                CancellationException cancellation = new CancellationException("cancelled");
                waiter.result = CompletableFuture.failedFuture(cancellation);
                assertSame(cancellation, failureOf(operation.run()));
            }
        }
    }

    @Test
    void synchronousWaitRegistrationFailurePreventsWrite() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (SoulseekClient client = loggedInClient(connection, waiter)) {
            RuntimeException expected = new RuntimeException("registration failed");
            waiter.synchronousFailure = expected;

            SoulseekClientException passwordFailure =
                    assertInstanceOf(SoulseekClientException.class, failureOf(client.changePasswordAsync("password")));
            assertSame(expected, passwordFailure.getCause());
            SoulseekClientException pingFailure =
                    assertInstanceOf(SoulseekClientException.class, failureOf(client.pingServerAsync()));
            assertSame(expected, pingFailure.getCause());
            assertEquals(0, connection.writeCount);
        }
    }

    private static SoulseekClient loggedInClient(ConnectionProbe connection, WaiterProbe waiter) {
        SoulseekClient client = new SoulseekClient(
                9999,
                null,
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
        return client;
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

    @SuppressWarnings("unchecked")
    private static <T> void complete(CompletableFuture<?> future, T value) {
        ((CompletableFuture<T>) future).complete(value);
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

    @FunctionalInterface
    private interface Operation {
        CompletableFuture<?> run();
    }

    private static final class ConnectionProbe {
        private dev.slsk.messaging.messages.IOutgoingMessage message;
        private CancellationToken token;
        private int writeCount;
        private CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        private RuntimeException synchronousFailure;
        private final IMessageConnection proxy = (IMessageConnection) Proxy.newProxyInstance(
                IMessageConnection.class.getClassLoader(), new Class<?>[] {IMessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("writeAsync")
                    && arguments.length == 2
                    && arguments[0] instanceof dev.slsk.messaging.messages.IOutgoingMessage outgoing) {
                writeCount++;
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
        private WaitKey key;
        private Class<?> resultType;
        private CancellationToken token;
        private int argumentCount;
        private int registrations;
        private CompletableFuture<?> result = CompletableFuture.completedFuture(null);
        private RuntimeException synchronousFailure;
        private final IWaiter proxy = (IWaiter)
                Proxy.newProxyInstance(IWaiter.class.getClassLoader(), new Class<?>[] {IWaiter.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("waitAsync") && (arguments.length == 3 || arguments.length == 4)) {
                registrations++;
                argumentCount = arguments.length;
                key = (WaitKey) arguments[0];
                resultType = arguments.length == 4 ? (Class<?>) arguments[1] : null;
                token = (CancellationToken) arguments[arguments.length - 1];
                if (synchronousFailure != null) {
                    throw synchronousFailure;
                }
                return result;
            }
            return defaultValue(method.getReturnType());
        }
    }
}
