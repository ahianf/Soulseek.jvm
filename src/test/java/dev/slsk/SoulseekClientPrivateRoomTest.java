// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.common.IWaiter;
import dev.slsk.common.WaitKey;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.messages.OutgoingMessage;
import dev.slsk.messaging.messages.PrivateRoomAddOperator;
import dev.slsk.messaging.messages.PrivateRoomAddUser;
import dev.slsk.messaging.messages.PrivateRoomDropMembershipCommand;
import dev.slsk.messaging.messages.PrivateRoomDropOwnershipCommand;
import dev.slsk.messaging.messages.PrivateRoomRemoveOperator;
import dev.slsk.messaging.messages.PrivateRoomRemoveUser;
import dev.slsk.network.MessageConnection;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class SoulseekClientPrivateRoomTest {
    @Test
    void registersExpectedWaitBeforeWritingExactCommand() {
        List<String> sequence = new ArrayList<>();
        WaiterProbe waiter = new WaiterProbe(sequence);
        ConnectionProbe connection = new ConnectionProbe(sequence);
        try (SoulseekClient client = loggedInClient(connection, waiter)) {
            CancellationTokenSource source = new CancellationTokenSource();
            CancellationToken token = source.getToken();
            List<Case> cases = List.of(
                    new Case(
                            () -> client.addPrivateRoomMemberAsync("room", "member", token),
                            new PrivateRoomAddUser("room", "member"),
                            new WaitKey(MessageCode.Server.PRIVATE_ROOM_ADD_USER, "room", "member")),
                    new Case(
                            () -> client.addPrivateRoomModeratorAsync("room", "moderator", token),
                            new PrivateRoomAddOperator("room", "moderator"),
                            new WaitKey(MessageCode.Server.PRIVATE_ROOM_ADD_OPERATOR, "room", "moderator")),
                    new Case(
                            () -> client.removePrivateRoomMemberAsync("room", "member", token),
                            new PrivateRoomRemoveUser("room", "member"),
                            new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVE_USER, "room", "member")),
                    new Case(
                            () -> client.removePrivateRoomModeratorAsync("room", "moderator", token),
                            new PrivateRoomRemoveOperator("room", "moderator"),
                            new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVE_OPERATOR, "room", "moderator")),
                    new Case(
                            () -> client.dropPrivateRoomMembershipAsync("room", token),
                            new PrivateRoomDropMembershipCommand("room"),
                            new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVED, "room")),
                    new Case(
                            () -> client.dropPrivateRoomOwnershipAsync("room", token),
                            new PrivateRoomDropOwnershipCommand("room"),
                            new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVED, "room")));

            for (Case testCase : cases) {
                waiter.result = new CompletableFuture<>();
                sequence.clear();

                CompletableFuture<Void> operation = testCase.operation.run();

                assertFalse(operation.isDone());
                assertEquals(List.of("wait", "write"), sequence);
                assertEquals(testCase.waitKey, waiter.key);
                assertSame(token, waiter.token);
                assertSame(token, connection.token);
                assertArrayEquals(testCase.message.toByteArray(), connection.message.toByteArray());
                waiter.result.complete(null);
                operation.join();
            }
        }
    }

    @Test
    void validatesArgumentsBeforeLoginState() {
        WaiterProbe waiter = new WaiterProbe(new ArrayList<>());
        ConnectionProbe connection = new ConnectionProbe(new ArrayList<>());
        try (SoulseekClient client = loggedInClient(connection, waiter)) {
            for (String bad : new String[] {null, "", " ", "\t"}) {
                assertThrows(IllegalArgumentException.class, () -> client.addPrivateRoomMemberAsync(bad, "user"));
                assertThrows(IllegalArgumentException.class, () -> client.addPrivateRoomMemberAsync("room", bad));
                assertThrows(IllegalArgumentException.class, () -> client.addPrivateRoomModeratorAsync(bad, "user"));
                assertThrows(IllegalArgumentException.class, () -> client.addPrivateRoomModeratorAsync("room", bad));
                assertThrows(IllegalArgumentException.class, () -> client.removePrivateRoomMemberAsync(bad, "user"));
                assertThrows(IllegalArgumentException.class, () -> client.removePrivateRoomMemberAsync("room", bad));
                assertThrows(IllegalArgumentException.class, () -> client.removePrivateRoomModeratorAsync(bad, "user"));
                assertThrows(IllegalArgumentException.class, () -> client.removePrivateRoomModeratorAsync("room", bad));
                assertThrows(IllegalArgumentException.class, () -> client.dropPrivateRoomMembershipAsync(bad));
                assertThrows(IllegalArgumentException.class, () -> client.dropPrivateRoomOwnershipAsync(bad));
            }

            client.setStateForTest(SoulseekClientStates.DISCONNECTED);
            for (Operation operation : operations(client)) {
                assertThrows(IllegalStateException.class, operation::run);
            }
            assertThrows(IllegalArgumentException.class, () -> client.addPrivateRoomMemberAsync(null, "user"));
        }
    }

    @Test
    void mapsWriteFailuresAndPreservesTimeoutAndCancellation() {
        WaiterProbe waiter = new WaiterProbe(new ArrayList<>());
        ConnectionProbe connection = new ConnectionProbe(new ArrayList<>());
        try (SoulseekClient client = loggedInClient(connection, waiter)) {
            for (Operation operation : operations(client)) {
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
    void mapsWaitFailuresAndPreservesTimeoutAndCancellation() {
        WaiterProbe waiter = new WaiterProbe(new ArrayList<>());
        ConnectionProbe connection = new ConnectionProbe(new ArrayList<>());
        try (SoulseekClient client = loggedInClient(connection, waiter)) {
            for (Operation operation : operations(client)) {
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
        WaiterProbe waiter = new WaiterProbe(new ArrayList<>());
        ConnectionProbe connection = new ConnectionProbe(new ArrayList<>());
        try (SoulseekClient client = loggedInClient(connection, waiter)) {
            RuntimeException expected = new RuntimeException("registration failed");
            waiter.synchronousFailure = expected;

            SoulseekClientException mapped = assertInstanceOf(
                    SoulseekClientException.class, failureOf(client.addPrivateRoomMemberAsync("room", "user")));

            assertSame(expected, mapped.getCause());
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

    private static List<Operation> operations(SoulseekClient client) {
        return List.of(
                () -> client.addPrivateRoomMemberAsync("room", "user"),
                () -> client.addPrivateRoomModeratorAsync("room", "user"),
                () -> client.removePrivateRoomMemberAsync("room", "user"),
                () -> client.removePrivateRoomModeratorAsync("room", "user"),
                () -> client.dropPrivateRoomMembershipAsync("room"),
                () -> client.dropPrivateRoomOwnershipAsync("room"));
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

    @FunctionalInterface
    private interface Operation {
        CompletableFuture<Void> run();
    }

    private record Case(Operation operation, OutgoingMessage message, WaitKey waitKey) {}

    private static final class ConnectionProbe {
        private final List<String> sequence;
        private OutgoingMessage message;
        private CancellationToken token;
        private int writeCount;
        private CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        private RuntimeException synchronousFailure;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private ConnectionProbe(List<String> sequence) {
            this.sequence = sequence;
        }

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("writeAsync")
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage outgoing) {
                sequence.add("write");
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
        private final List<String> sequence;
        private WaitKey key;
        private CancellationToken token;
        private CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        private RuntimeException synchronousFailure;
        private final IWaiter proxy = (IWaiter)
                Proxy.newProxyInstance(IWaiter.class.getClassLoader(), new Class<?>[] {IWaiter.class}, this::invoke);

        private WaiterProbe(List<String> sequence) {
            this.sequence = sequence;
        }

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("waitAsync") && arguments.length == 3) {
                sequence.add("wait");
                key = (WaitKey) arguments[0];
                token = (CancellationToken) arguments[2];
                if (synchronousFailure != null) {
                    throw synchronousFailure;
                }
                return result;
            }
            return defaultValue(method.getReturnType());
        }
    }
}
