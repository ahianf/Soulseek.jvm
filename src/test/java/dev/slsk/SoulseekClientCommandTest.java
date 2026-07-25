// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.messaging.messages.AcknowledgePrivateMessageCommand;
import dev.slsk.messaging.messages.AcknowledgePrivilegeNotificationCommand;
import dev.slsk.messaging.messages.OutgoingMessage;
import dev.slsk.messaging.messages.PrivateMessageCommand;
import dev.slsk.messaging.messages.RoomMessageCommand;
import dev.slsk.messaging.messages.SendUploadSpeedCommand;
import dev.slsk.messaging.messages.SetOnlineStatusCommand;
import dev.slsk.messaging.messages.SetRoomTickerCommand;
import dev.slsk.messaging.messages.SetSharedCountsCommand;
import dev.slsk.messaging.messages.StartPublicChatCommand;
import dev.slsk.messaging.messages.StopPublicChatCommand;
import dev.slsk.messaging.messages.UnwatchUserCommand;
import dev.slsk.network.MessageConnection;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class SoulseekClientCommandTest {
    @Test
    void sendsExpectedCommandsAndForwardsCancellationSignal() {
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection)) {
            CancellationController source = new CancellationController();
            CancellationSignal token = source.getSignal();

            client.sendPrivateMessage("alice", "private", token);
            client.sendRoomMessage("room", "public", token);
            client.sendUploadSpeed(1234, token);
            client.setRoomTicker("room", "ticker", token);
            client.setSharedCounts(12, 34, token);
            client.setStatus(UserPresence.AWAY, token);
            client.startPublicChat(token);
            client.stopPublicChat(token);
            client.unwatchUser("bob", token);

            assertEquals(9, connection.messages.size());
            PrivateMessageCommand privateMessage =
                    assertInstanceOf(PrivateMessageCommand.class, connection.messages.get(0));
            assertEquals("alice", privateMessage.getUsername());
            assertEquals("private", privateMessage.getMessage());
            RoomMessageCommand roomMessage = assertInstanceOf(RoomMessageCommand.class, connection.messages.get(1));
            assertEquals("room", roomMessage.getRoomName());
            assertEquals("public", roomMessage.getMessage());
            assertEquals(
                    1234,
                    assertInstanceOf(SendUploadSpeedCommand.class, connection.messages.get(2))
                            .getSpeed());
            SetRoomTickerCommand ticker = assertInstanceOf(SetRoomTickerCommand.class, connection.messages.get(3));
            assertEquals("room", ticker.getRoomName());
            assertEquals("ticker", ticker.getMessage());
            SetSharedCountsCommand counts = assertInstanceOf(SetSharedCountsCommand.class, connection.messages.get(4));
            assertEquals(12, counts.getDirectoryCount());
            assertEquals(34, counts.getFileCount());
            assertEquals(
                    UserPresence.AWAY,
                    assertInstanceOf(SetOnlineStatusCommand.class, connection.messages.get(5))
                            .getStatus());
            assertInstanceOf(StartPublicChatCommand.class, connection.messages.get(6));
            assertInstanceOf(StopPublicChatCommand.class, connection.messages.get(7));
            assertEquals(
                    "bob",
                    assertInstanceOf(UnwatchUserCommand.class, connection.messages.get(8))
                            .getUsername());
            connection.tokens.forEach(recorded -> assertSame(token, recorded));
        }
    }

    @Test
    void validatesTextRangeAndStateInSourceOrder() {
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection)) {
            assertThrows(IllegalArgumentException.class, () -> client.sendPrivateMessage(" ", "message"));
            assertThrows(IllegalArgumentException.class, () -> client.sendPrivateMessage("user", ""));
            assertThrows(IllegalArgumentException.class, () -> client.sendRoomMessage(null, "message"));
            assertThrows(IllegalArgumentException.class, () -> client.sendRoomMessage("room", null));
            assertThrows(IllegalArgumentException.class, () -> client.setRoomTicker("\t", "message"));
            assertThrows(IllegalArgumentException.class, () -> client.setRoomTicker("room", ""));
            assertThrows(IllegalArgumentException.class, () -> client.setSharedCounts(-1, 0));
            assertThrows(IllegalArgumentException.class, () -> client.setSharedCounts(0, -1));
            assertThrows(IllegalArgumentException.class, () -> client.sendUploadSpeed(0));
            assertThrows(IllegalArgumentException.class, () -> client.unwatchUser(" "));

            client.sendPrivateMessage("user", " ");
            client.sendRoomMessage("room", " ");
            client.setRoomTicker("room", " ");

            client.setStateForTest(SoulseekClientState.DISCONNECTED);
            for (Operation operation : operations(client)) {
                assertThrows(IllegalStateException.class, operation::run);
            }
            assertThrows(IllegalArgumentException.class, () -> client.sendPrivateMessage(null, "message"));
            assertThrows(IllegalArgumentException.class, () -> client.setSharedCounts(-1, 0));
            assertThrows(IllegalStateException.class, () -> client.sendUploadSpeed(0));
        }
    }

    @Test
    void wrapsEveryOrdinaryWriteFailureIncludingSynchronousOnes() {
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection)) {
            for (Operation operation : operations(client)) {
                RuntimeException expected = new RuntimeException("write failed");
                connection.synchronousFailure = expected;

                Throwable actual = failureOf(() -> operation.run());

                SoulseekClientException mapped = assertInstanceOf(SoulseekClientException.class, actual);
                assertSame(expected, mapped.getCause());
                connection.synchronousFailure = null;
            }
        }
    }

    @Test
    void preservesTimeoutAndCancellationForEveryWrite() {
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection)) {
            for (Operation operation : operations(client)) {
                TimeoutException timeout = new TimeoutException("timed out");
                connection.result = CompletableFuture.failedFuture(timeout);
                assertSame(
                        timeout,
                        assertInstanceOf(NoResponseException.class, failureOf(() -> operation.run()))
                                .getCause());

                CancellationException cancellation = new CancellationException("cancelled");
                connection.result = CompletableFuture.failedFuture(cancellation);
                assertSame(cancellation, failureOf(() -> operation.run()));
            }
        }
    }

    @Test
    void acknowledgementCommandsAlsoUseGuardedWritePath() {
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection)) {
            client.acknowledgePrivateMessage(123);
            client.acknowledgePrivilegeNotification(456);
            assertEquals(
                    123,
                    assertInstanceOf(AcknowledgePrivateMessageCommand.class, connection.messages.get(0))
                            .getId());
            assertEquals(
                    456,
                    assertInstanceOf(AcknowledgePrivilegeNotificationCommand.class, connection.messages.get(1))
                            .getId());

            RuntimeException expected = new RuntimeException("synchronous");
            connection.synchronousFailure = expected;
            SoulseekClientException mapped = assertInstanceOf(
                    SoulseekClientException.class, failureOf(() -> client.acknowledgePrivateMessage(789)));
            assertSame(expected, mapped.getCause());
        }
    }

    private static DefaultSoulseekClient loggedInClient(ConnectionProbe connection) {
        DefaultSoulseekClient client = new DefaultSoulseekClient(9999);
        client.setServerConnectionForTest(connection.proxy);
        client.setStateForTest(SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGED_IN));
        return client;
    }

    private static List<Operation> operations(DefaultSoulseekClient client) {
        return List.of(
                () -> client.sendPrivateMessage("user", "message"),
                () -> client.sendRoomMessage("room", "message"),
                () -> client.sendUploadSpeed(1),
                () -> client.setRoomTicker("room", "message"),
                () -> client.setSharedCounts(1, 2),
                () -> client.setStatus(UserPresence.ONLINE),
                client::startPublicChat,
                client::stopPublicChat,
                () -> client.unwatchUser("user"),
                () -> client.acknowledgePrivateMessage(1),
                () -> client.acknowledgePrivilegeNotification(1));
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
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0F;
        }
        if (type == double.class) {
            return 0D;
        }
        if (type == char.class) {
            return '\0';
        }
        return null;
    }

    /** A blocking client call under test; void now that the API is blocking. */
    @FunctionalInterface
    private interface Operation {
        void run();
    }

    private static final class ConnectionProbe {
        private final List<OutgoingMessage> messages = new ArrayList<>();
        private final List<CancellationSignal> tokens = new ArrayList<>();
        private CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        private RuntimeException synchronousFailure;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("writeAsync")
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage message) {
                messages.add(message);
                tokens.add((CancellationSignal) arguments[1]);
                if (synchronousFailure != null) {
                    throw synchronousFailure;
                }
                return result;
            }
            return defaultValue(method.getReturnType());
        }
    }
}
