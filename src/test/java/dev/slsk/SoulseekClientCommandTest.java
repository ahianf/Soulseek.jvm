// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class SoulseekClientCommandTest {
    @Test
    void sendsExpectedCommandsAndForwardsCancellationToken() {
        ConnectionProbe connection = new ConnectionProbe();
        try (SoulseekClient client = loggedInClient(connection)) {
            CancellationTokenSource source = new CancellationTokenSource();
            CancellationToken token = source.getToken();

            client.sendPrivateMessageAsync("alice", "private", token).join();
            client.sendRoomMessageAsync("room", "public", token).join();
            client.sendUploadSpeedAsync(1234, token).join();
            client.setRoomTickerAsync("room", "ticker", token).join();
            client.setSharedCountsAsync(12, 34, token).join();
            client.setStatusAsync(UserPresence.AWAY, token).join();
            client.startPublicChatAsync(token).join();
            client.stopPublicChatAsync(token).join();
            client.unwatchUserAsync("bob", token).join();

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
        try (SoulseekClient client = loggedInClient(connection)) {
            assertThrows(IllegalArgumentException.class, () -> client.sendPrivateMessageAsync(" ", "message"));
            assertThrows(IllegalArgumentException.class, () -> client.sendPrivateMessageAsync("user", ""));
            assertThrows(IllegalArgumentException.class, () -> client.sendRoomMessageAsync(null, "message"));
            assertThrows(IllegalArgumentException.class, () -> client.sendRoomMessageAsync("room", null));
            assertThrows(IllegalArgumentException.class, () -> client.setRoomTickerAsync("\t", "message"));
            assertThrows(IllegalArgumentException.class, () -> client.setRoomTickerAsync("room", ""));
            assertThrows(IllegalArgumentException.class, () -> client.setSharedCountsAsync(-1, 0));
            assertThrows(IllegalArgumentException.class, () -> client.setSharedCountsAsync(0, -1));
            assertThrows(IllegalArgumentException.class, () -> client.sendUploadSpeedAsync(0));
            assertThrows(IllegalArgumentException.class, () -> client.unwatchUserAsync(" "));

            client.sendPrivateMessageAsync("user", " ").join();
            client.sendRoomMessageAsync("room", " ").join();
            client.setRoomTickerAsync("room", " ").join();

            client.setStateForTest(SoulseekClientStates.DISCONNECTED);
            for (Operation operation : operations(client)) {
                assertThrows(IllegalStateException.class, operation::run);
            }
            assertThrows(IllegalArgumentException.class, () -> client.sendPrivateMessageAsync(null, "message"));
            assertThrows(IllegalArgumentException.class, () -> client.setSharedCountsAsync(-1, 0));
            assertThrows(IllegalStateException.class, () -> client.sendUploadSpeedAsync(0));
        }
    }

    @Test
    void wrapsEveryOrdinaryWriteFailureIncludingSynchronousOnes() {
        ConnectionProbe connection = new ConnectionProbe();
        try (SoulseekClient client = loggedInClient(connection)) {
            for (Operation operation : operations(client)) {
                RuntimeException expected = new RuntimeException("write failed");
                connection.synchronousFailure = expected;

                Throwable actual = failureOf(operation.run());

                SoulseekClientException mapped = assertInstanceOf(SoulseekClientException.class, actual);
                assertSame(expected, mapped.getCause());
                connection.synchronousFailure = null;
            }
        }
    }

    @Test
    void preservesTimeoutAndCancellationForEveryWrite() {
        ConnectionProbe connection = new ConnectionProbe();
        try (SoulseekClient client = loggedInClient(connection)) {
            for (Operation operation : operations(client)) {
                TimeoutException timeout = new TimeoutException("timed out");
                connection.result = CompletableFuture.failedFuture(timeout);
                assertSame(timeout, failureOf(operation.run()));

                CancellationException cancellation = new CancellationException("cancelled");
                connection.result = CompletableFuture.failedFuture(cancellation);
                assertSame(cancellation, failureOf(operation.run()));
            }
        }
    }

    @Test
    void acknowledgementCommandsAlsoUseGuardedWritePath() {
        ConnectionProbe connection = new ConnectionProbe();
        try (SoulseekClient client = loggedInClient(connection)) {
            client.acknowledgePrivateMessageAsync(123).join();
            client.acknowledgePrivilegeNotificationAsync(456).join();
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
                    SoulseekClientException.class, failureOf(client.acknowledgePrivateMessageAsync(789)));
            assertSame(expected, mapped.getCause());
        }
    }

    private static SoulseekClient loggedInClient(ConnectionProbe connection) {
        SoulseekClient client = new SoulseekClient(9999);
        client.setServerConnectionForTest(connection.proxy);
        client.setStateForTest(SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGED_IN));
        return client;
    }

    private static List<Operation> operations(SoulseekClient client) {
        return List.of(
                () -> client.sendPrivateMessageAsync("user", "message"),
                () -> client.sendRoomMessageAsync("room", "message"),
                () -> client.sendUploadSpeedAsync(1),
                () -> client.setRoomTickerAsync("room", "message"),
                () -> client.setSharedCountsAsync(1, 2),
                () -> client.setStatusAsync(UserPresence.ONLINE),
                client::startPublicChatAsync,
                client::stopPublicChatAsync,
                () -> client.unwatchUserAsync("user"),
                () -> client.acknowledgePrivateMessageAsync(1),
                () -> client.acknowledgePrivilegeNotificationAsync(1));
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

    @FunctionalInterface
    private interface Operation {
        CompletableFuture<Void> run();
    }

    private static final class ConnectionProbe {
        private final List<OutgoingMessage> messages = new ArrayList<>();
        private final List<CancellationToken> tokens = new ArrayList<>();
        private CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        private RuntimeException synchronousFailure;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("writeAsync")
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage message) {
                messages.add(message);
                tokens.add((CancellationToken) arguments[1]);
                if (synchronousFailure != null) {
                    throw synchronousFailure;
                }
                return result;
            }
            return defaultValue(method.getReturnType());
        }
    }
}
