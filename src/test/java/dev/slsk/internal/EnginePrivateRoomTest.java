// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.CancellationController;
import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.internal.common.Outcomes;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.messaging.messages.PrivateRoomAddOperator;
import dev.slsk.internal.messaging.messages.PrivateRoomAddUser;
import dev.slsk.internal.messaging.messages.PrivateRoomDropMembershipCommand;
import dev.slsk.internal.messaging.messages.PrivateRoomDropOwnershipCommand;
import dev.slsk.internal.messaging.messages.PrivateRoomRemoveOperator;
import dev.slsk.internal.messaging.messages.PrivateRoomRemoveUser;
import dev.slsk.internal.network.MessageConnection;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class EnginePrivateRoomTest {
    @Test
    void registersExpectedWaitBeforeWritingExactCommand() {
        List<String> sequence = new ArrayList<>();
        WaiterProbe waiter = new WaiterProbe(sequence);
        ConnectionProbe connection = new ConnectionProbe(sequence);
        try (SoulseekEngine client = loggedInClient(connection, waiter)) {
            CancellationController source = new CancellationController();
            CancellationSignal token = source.getSignal();
            List<Case> cases = List.of(
                    new Case(
                            () -> client.rooms().addPrivateRoomMember("room", "member", token),
                            new PrivateRoomAddUser("room", "member"),
                            new WaitKey(MessageCode.Server.PRIVATE_ROOM_ADD_USER, "room", "member")),
                    new Case(
                            () -> client.rooms().addPrivateRoomModerator("room", "moderator", token),
                            new PrivateRoomAddOperator("room", "moderator"),
                            new WaitKey(MessageCode.Server.PRIVATE_ROOM_ADD_OPERATOR, "room", "moderator")),
                    new Case(
                            () -> client.rooms().removePrivateRoomMember("room", "member", token),
                            new PrivateRoomRemoveUser("room", "member"),
                            new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVE_USER, "room", "member")),
                    new Case(
                            () -> client.rooms().removePrivateRoomModerator("room", "moderator", token),
                            new PrivateRoomRemoveOperator("room", "moderator"),
                            new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVE_OPERATOR, "room", "moderator")),
                    new Case(
                            () -> client.rooms().dropPrivateRoomMembership("room", token),
                            new PrivateRoomDropMembershipCommand("room"),
                            new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVED, "room")),
                    new Case(
                            () -> client.rooms().dropPrivateRoomOwnership("room", token),
                            new PrivateRoomDropOwnershipCommand("room"),
                            new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVED, "room")));

            for (Case testCase : cases) {
                waiter.result = new CompletableFuture<>();
                sequence.clear();

                CompletableFuture<Void> operation = inBackground(() -> testCase.operation.run());

                // The call runs on another thread now, so wait for it to reach
                // the probe before asserting the order it did things in.
                waitUntilSequence(sequence, 2);
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
        try (SoulseekEngine client = loggedInClient(connection, waiter)) {
            for (String bad : new String[] {null, "", " ", "\t"}) {
                assertThrows(
                        IllegalArgumentException.class, () -> client.rooms().addPrivateRoomMember(bad, "user"));
                assertThrows(
                        IllegalArgumentException.class, () -> client.rooms().addPrivateRoomMember("room", bad));
                assertThrows(
                        IllegalArgumentException.class, () -> client.rooms().addPrivateRoomModerator(bad, "user"));
                assertThrows(
                        IllegalArgumentException.class, () -> client.rooms().addPrivateRoomModerator("room", bad));
                assertThrows(
                        IllegalArgumentException.class, () -> client.rooms().removePrivateRoomMember(bad, "user"));
                assertThrows(
                        IllegalArgumentException.class, () -> client.rooms().removePrivateRoomMember("room", bad));
                assertThrows(
                        IllegalArgumentException.class, () -> client.rooms().removePrivateRoomModerator(bad, "user"));
                assertThrows(
                        IllegalArgumentException.class, () -> client.rooms().removePrivateRoomModerator("room", bad));
                assertThrows(
                        IllegalArgumentException.class, () -> client.rooms().dropPrivateRoomMembership(bad));
                assertThrows(
                        IllegalArgumentException.class, () -> client.rooms().dropPrivateRoomOwnership(bad));
            }

            client.setStateForTest(SoulseekClientState.DISCONNECTED);
            for (Operation operation : operations(client)) {
                assertThrows(IllegalStateException.class, operation::run);
            }
            assertThrows(IllegalArgumentException.class, () -> client.rooms().addPrivateRoomMember(null, "user"));
        }
    }

    @Test
    void mapsWriteFailuresAndPreservesTimeoutAndCancellation() {
        WaiterProbe waiter = new WaiterProbe(new ArrayList<>());
        ConnectionProbe connection = new ConnectionProbe(new ArrayList<>());
        try (SoulseekEngine client = loggedInClient(connection, waiter)) {
            for (Operation operation : operations(client)) {
                RuntimeException expected = new RuntimeException("write failed");
                connection.synchronousFailure = expected;
                SoulseekClientException mapped =
                        assertInstanceOf(SoulseekClientException.class, failureOf(() -> operation.run()));
                assertSame(expected, mapped.getCause());
                connection.synchronousFailure = null;

                TimeoutException timeout = new TimeoutException("timed out");
                connection.result = CompletableFuture.failedFuture(timeout);
                assertSame(
                        timeout,
                        assertInstanceOf(NoResponseException.class, failureOf(() -> operation.run()))
                                .getCause());

                CancellationException cancellation = new CancellationException("cancelled");
                connection.result = CompletableFuture.failedFuture(cancellation);
                assertSame(cancellation, failureOf(() -> operation.run()));
                connection.result = CompletableFuture.completedFuture(null);
            }
        }
    }

    @Test
    void mapsWaitFailuresAndPreservesTimeoutAndCancellation() {
        WaiterProbe waiter = new WaiterProbe(new ArrayList<>());
        ConnectionProbe connection = new ConnectionProbe(new ArrayList<>());
        try (SoulseekEngine client = loggedInClient(connection, waiter)) {
            for (Operation operation : operations(client)) {
                RuntimeException expected = new RuntimeException("wait failed");
                waiter.result = CompletableFuture.failedFuture(expected);
                SoulseekClientException mapped =
                        assertInstanceOf(SoulseekClientException.class, failureOf(() -> operation.run()));
                assertSame(expected, mapped.getCause());

                TimeoutException timeout = new TimeoutException("timed out");
                waiter.result = CompletableFuture.failedFuture(timeout);
                assertSame(
                        timeout,
                        assertInstanceOf(NoResponseException.class, failureOf(() -> operation.run()))
                                .getCause());

                CancellationException cancellation = new CancellationException("cancelled");
                waiter.result = CompletableFuture.failedFuture(cancellation);
                assertSame(cancellation, failureOf(() -> operation.run()));
            }
        }
    }

    @Test
    void synchronousWaitRegistrationFailurePreventsWrite() {
        WaiterProbe waiter = new WaiterProbe(new ArrayList<>());
        ConnectionProbe connection = new ConnectionProbe(new ArrayList<>());
        try (SoulseekEngine client = loggedInClient(connection, waiter)) {
            RuntimeException expected = new RuntimeException("registration failed");
            waiter.synchronousFailure = expected;

            SoulseekClientException mapped = assertInstanceOf(
                    SoulseekClientException.class,
                    failureOf(() -> client.rooms().addPrivateRoomMember("room", "user")));

            assertSame(expected, mapped.getCause());
            assertEquals(0, connection.writeCount);
        }
    }

    private static SoulseekEngine loggedInClient(ConnectionProbe connection, WaiterProbe waiter) {
        SoulseekEngine client = new SoulseekEngine(
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
        client.setStateForTest(SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGED_IN));
        return client;
    }

    private static List<Operation> operations(SoulseekEngine client) {
        return List.of(
                () -> client.rooms().addPrivateRoomMember("room", "user"),
                () -> client.rooms().addPrivateRoomModerator("room", "user"),
                () -> client.rooms().removePrivateRoomMember("room", "user"),
                () -> client.rooms().removePrivateRoomModerator("room", "user"),
                () -> client.rooms().dropPrivateRoomMembership("room"),
                () -> client.rooms().dropPrivateRoomOwnership("room"));
    }

    /** Waits for the background caller to record {@code count} steps. */
    private static void waitUntilSequence(List<String> sequence, int count) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (sequence.size() < count && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }

    /** Runs a blocking call on a virtual thread so the test can observe it mid-flight. */
    private static CompletableFuture<Void> inBackground(Runnable call) {
        return CompletableFuture.runAsync(call, java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());
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

    /** A blocking client call under test; void now that the API is blocking. */
    @FunctionalInterface
    private interface Operation {
        void run();
    }

    private record Case(Operation operation, OutgoingMessage message, WaitKey waitKey) {}

    private static final class ConnectionProbe {
        private final List<String> sequence;
        private OutgoingMessage message;
        private CancellationSignal token;
        private int writeCount;
        private CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        private RuntimeException synchronousFailure;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private ConnectionProbe(List<String> sequence) {
            this.sequence = sequence;
        }

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("write")
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage outgoing) {
                sequence.add("write");
                writeCount++;
                message = outgoing;
                token = (CancellationSignal) arguments[1];
                if (synchronousFailure != null) {
                    throw synchronousFailure;
                }
                Outcomes.raise(result);
                return null;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class WaiterProbe {
        private final List<String> sequence;
        private WaitKey key;
        private CancellationSignal token;
        private CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        private RuntimeException synchronousFailure;
        private final Waiter proxy = (Waiter)
                Proxy.newProxyInstance(Waiter.class.getClassLoader(), new Class<?>[] {Waiter.class}, this::invoke);

        private WaiterProbe(List<String> sequence) {
            this.sequence = sequence;
        }

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("register") && arguments.length == 3) {
                sequence.add("wait");
                key = (WaitKey) arguments[0];
                token = (CancellationSignal) arguments[2];
                if (synchronousFailure != null) {
                    throw synchronousFailure;
                }
                return (Wait<Object>) () -> Outcomes.raise(result);
            }
            return defaultValue(method.getReturnType());
        }
    }
}
