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

import dev.slsk.common.WaitKey;
import dev.slsk.common.Waiter;
import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.RoomJoinForbiddenException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.UserNotFoundException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.messages.CheckPrivilegesRequest;
import dev.slsk.messaging.messages.GivePrivilegesCommand;
import dev.slsk.messaging.messages.JoinRoomRequest;
import dev.slsk.messaging.messages.LeaveRoomRequest;
import dev.slsk.messaging.messages.NewPassword;
import dev.slsk.messaging.messages.RoomListRequest;
import dev.slsk.messaging.messages.ServerPing;
import dev.slsk.messaging.messages.UserPrivilegesRequest;
import dev.slsk.messaging.messages.UserStatisticsRequest;
import dev.slsk.messaging.messages.UserStatusRequest;
import dev.slsk.messaging.messages.WatchUserRequest;
import dev.slsk.messaging.messages.WatchUserResponse;
import dev.slsk.network.MessageConnection;
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
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            CancellationController source = new CancellationController();
            CancellationSignal token = source.getSignal();
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
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            CancellationController source = new CancellationController();
            CancellationSignal token = source.getSignal();
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
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            CancellationController source = new CancellationController();
            CancellationSignal token = source.getSignal();
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
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            CancellationController source = new CancellationController();
            CancellationSignal token = source.getSignal();

            client.grantUserPrivilegesAsync("alice", 7, token).join();

            GivePrivilegesCommand command = assertInstanceOf(GivePrivilegesCommand.class, connection.message);
            assertEquals("alice", command.getUsername());
            assertEquals(7, command.getDays());
            assertSame(token, connection.token);
            assertEquals(0, waiter.registrations);
        }
    }

    @Test
    void userLookupsReturnTypedCorrelatedResponses() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            CancellationController source = new CancellationController();
            CancellationSignal token = source.getSignal();

            waiter.result = CompletableFuture.completedFuture(true);
            assertTrue(client.getUserPrivilegedAsync("alice", token).join());
            assertEquals(new WaitKey(MessageCode.Server.USER_PRIVILEGES, "alice"), waiter.key);
            assertSame(Boolean.class, waiter.resultType);
            assertEquals(
                    "alice",
                    assertInstanceOf(UserPrivilegesRequest.class, connection.message)
                            .getUsername());

            UserStatistics statistics = new UserStatistics("alice", 10, 20, 30, 40);
            waiter.result = CompletableFuture.completedFuture(statistics);
            assertSame(statistics, client.getUserStatisticsAsync("alice", token).join());
            assertEquals(new WaitKey(MessageCode.Server.GET_USER_STATS, "alice"), waiter.key);
            assertSame(UserStatistics.class, waiter.resultType);
            assertEquals(
                    "alice",
                    assertInstanceOf(UserStatisticsRequest.class, connection.message)
                            .getUsername());

            UserStatus status = new UserStatus("alice", UserPresence.AWAY, true);
            waiter.result = CompletableFuture.completedFuture(status);
            assertSame(status, client.getUserStatusAsync("alice", token).join());
            assertEquals(new WaitKey(MessageCode.Server.GET_STATUS, "alice"), waiter.key);
            assertSame(UserStatus.class, waiter.resultType);
            assertEquals(
                    "alice",
                    assertInstanceOf(UserStatusRequest.class, connection.message)
                            .getUsername());
            assertSame(token, waiter.token);
            assertSame(token, connection.token);
        }
    }

    @Test
    void watchUserReturnsDataOrReportsMissingUser() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            UserData data = new UserData("alice", UserPresence.ONLINE, 10, 20, 30, 40, "CL");
            waiter.result = CompletableFuture.completedFuture(new WatchUserResponse("alice", true, data));

            assertSame(data, client.watchUserAsync("alice").join());
            assertEquals(new WaitKey(MessageCode.Server.WATCH_USER, "alice"), waiter.key);
            assertSame(WatchUserResponse.class, waiter.resultType);
            assertEquals(
                    "alice",
                    assertInstanceOf(WatchUserRequest.class, connection.message).getUsername());

            waiter.result = CompletableFuture.completedFuture(new WatchUserResponse("missing", false));
            assertInstanceOf(UserNotFoundException.class, failureOf(client.watchUserAsync("missing")));
        }
    }

    @Test
    void roomListAndMembershipUseExpectedCorrelations() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            CancellationController source = new CancellationController();
            CancellationSignal token = source.getSignal();

            RoomList roomList = new RoomList(List.of(), List.of(), List.of(), List.of());
            waiter.result = CompletableFuture.completedFuture(roomList);
            assertSame(roomList, client.getRoomListAsync(token).join());
            assertEquals(new WaitKey(MessageCode.Server.ROOM_LIST), waiter.key);
            assertSame(RoomList.class, waiter.resultType);
            assertInstanceOf(RoomListRequest.class, connection.message);

            RoomData roomData = new RoomData("room", List.of(), true);
            waiter.result = CompletableFuture.completedFuture(roomData);
            assertSame(roomData, client.joinRoomAsync("room", true, token).join());
            assertEquals(new WaitKey(MessageCode.Server.JOIN_ROOM, "room"), waiter.key);
            assertSame(RoomData.class, waiter.resultType);
            JoinRoomRequest join = assertInstanceOf(JoinRoomRequest.class, connection.message);
            assertEquals("room", join.getRoomName());
            assertTrue(join.isPrivate());

            waiter.result = CompletableFuture.completedFuture(null);
            client.leaveRoomAsync("room", token).join();
            assertEquals(new WaitKey(MessageCode.Server.LEAVE_ROOM, "room"), waiter.key);
            assertEquals(3, waiter.argumentCount);
            assertEquals(
                    "room",
                    assertInstanceOf(LeaveRoomRequest.class, connection.message).getRoomName());
            assertSame(token, waiter.token);
            assertSame(token, connection.token);
        }
    }

    @Test
    void roomWaitTimeoutsTranslateButWriteTimeoutsDoNot() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            waiter.result = CompletableFuture.failedFuture(new TimeoutException("no response"));
            assertInstanceOf(NoResponseException.class, failureOf(client.joinRoomAsync("room")));
            assertInstanceOf(NoResponseException.class, failureOf(client.leaveRoomAsync("room")));

            waiter.result = new CompletableFuture<>();
            TimeoutException writeTimeout = new TimeoutException("write timed out");
            connection.result = CompletableFuture.failedFuture(writeTimeout);
            assertSame(writeTimeout, failureOf(client.joinRoomAsync("room")));
            assertSame(writeTimeout, failureOf(client.leaveRoomAsync("room")));
        }
    }

    @Test
    void joinPreservesServerRejectionAndRoomFailuresMapCorrectly() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            RoomJoinForbiddenException forbidden = new RoomJoinForbiddenException("forbidden");
            waiter.result = CompletableFuture.failedFuture(forbidden);
            assertSame(forbidden, failureOf(client.joinRoomAsync("room")));

            RoomJoinForbiddenException synchronousForbidden = new RoomJoinForbiddenException("synchronous");
            waiter.synchronousFailure = synchronousForbidden;
            int writesBeforeFailure = connection.writeCount;
            assertSame(synchronousForbidden, failureOf(client.joinRoomAsync("room")));
            assertEquals(writesBeforeFailure, connection.writeCount);
            waiter.synchronousFailure = null;

            waiter.result = CompletableFuture.completedFuture(new RoomData("room", List.of()));
            RuntimeException expected = new RuntimeException("write failed");
            connection.synchronousFailure = expected;
            SoulseekClientException joinFailure =
                    assertInstanceOf(SoulseekClientException.class, failureOf(client.joinRoomAsync("room")));
            assertSame(expected, joinFailure.getCause());
            SoulseekClientException leaveFailure =
                    assertInstanceOf(SoulseekClientException.class, failureOf(client.leaveRoomAsync("room")));
            assertSame(expected, leaveFailure.getCause());
        }
    }

    @Test
    void preservesUserSpecificFailuresRequiredBySource() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            UserOfflineException offline = new UserOfflineException("offline");
            waiter.result = CompletableFuture.failedFuture(offline);
            assertSame(offline, failureOf(client.getUserPrivilegedAsync("alice")));
            assertSame(offline, failureOf(client.getUserStatusAsync("alice")));

            UserNotFoundException notFound = new UserNotFoundException("missing");
            waiter.result = CompletableFuture.failedFuture(notFound);
            assertSame(notFound, failureOf(client.watchUserAsync("alice")));

            RuntimeException expected = new RuntimeException("statistics failed");
            waiter.result = CompletableFuture.failedFuture(expected);
            SoulseekClientException mapped =
                    assertInstanceOf(SoulseekClientException.class, failureOf(client.getUserStatisticsAsync("alice")));
            assertSame(expected, mapped.getCause());
        }
    }

    @Test
    void validatesArgumentsAndLoginStateInSourceOrder() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            for (String bad : new String[] {null, "", " ", "\t"}) {
                assertThrows(IllegalArgumentException.class, () -> client.changePasswordAsync(bad));
                assertThrows(IllegalArgumentException.class, () -> client.grantUserPrivilegesAsync(bad, 1));
            }
            assertThrows(IllegalArgumentException.class, () -> client.grantUserPrivilegesAsync("user", 0));
            assertThrows(IllegalArgumentException.class, () -> client.grantUserPrivilegesAsync("user", -1));
            for (String bad : new String[] {null, "", " ", "\t"}) {
                assertThrows(IllegalArgumentException.class, () -> client.getUserPrivilegedAsync(bad));
                assertThrows(IllegalArgumentException.class, () -> client.getUserStatisticsAsync(bad));
                assertThrows(IllegalArgumentException.class, () -> client.getUserStatusAsync(bad));
                assertThrows(IllegalArgumentException.class, () -> client.watchUserAsync(bad));
            }
            for (String bad : new String[] {null, "", " ", "\t"}) {
                assertThrows(IllegalArgumentException.class, () -> client.joinRoomAsync(bad));
                assertThrows(IllegalArgumentException.class, () -> client.leaveRoomAsync(bad));
            }

            client.setStateForTest(SoulseekClientState.DISCONNECTED);
            assertThrows(IllegalStateException.class, () -> client.changePasswordAsync("password"));
            assertThrows(IllegalStateException.class, client::getPrivilegesAsync);
            assertThrows(IllegalStateException.class, client::pingServerAsync);
            assertThrows(IllegalStateException.class, () -> client.grantUserPrivilegesAsync("user", 1));
            assertThrows(IllegalStateException.class, () -> client.getUserPrivilegedAsync("user"));
            assertThrows(IllegalStateException.class, () -> client.getUserStatisticsAsync("user"));
            assertThrows(IllegalStateException.class, () -> client.getUserStatusAsync("user"));
            assertThrows(IllegalStateException.class, () -> client.watchUserAsync("user"));
            assertThrows(IllegalStateException.class, client::getRoomListAsync);
            assertThrows(IllegalStateException.class, () -> client.joinRoomAsync("room"));
            assertThrows(IllegalStateException.class, () -> client.leaveRoomAsync("room"));
            assertThrows(IllegalArgumentException.class, () -> client.changePasswordAsync(null));
            assertThrows(IllegalArgumentException.class, () -> client.grantUserPrivilegesAsync(null, 0));
        }
    }

    @Test
    void mapsOrdinaryFailuresAndPreservesTimeoutAndCancellation() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            List<Operation> operations = List.of(
                    () -> client.changePasswordAsync("password"),
                    client::getPrivilegesAsync,
                    client::pingServerAsync,
                    () -> client.grantUserPrivilegesAsync("user", 1),
                    () -> client.getUserPrivilegedAsync("user"),
                    () -> client.getUserStatisticsAsync("user"),
                    () -> client.getUserStatusAsync("user"),
                    () -> client.watchUserAsync("user"),
                    client::getRoomListAsync,
                    () -> client.joinRoomAsync("room"),
                    () -> client.leaveRoomAsync("room"));
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
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            List<Operation> operations = List.of(
                    () -> client.changePasswordAsync("password"),
                    client::getPrivilegesAsync,
                    client::pingServerAsync,
                    () -> client.getUserPrivilegedAsync("user"),
                    () -> client.getUserStatisticsAsync("user"),
                    () -> client.getUserStatusAsync("user"),
                    () -> client.watchUserAsync("user"),
                    client::getRoomListAsync);
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
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
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

    private static DefaultSoulseekClient loggedInClient(ConnectionProbe connection, WaiterProbe waiter) {
        DefaultSoulseekClient client = new DefaultSoulseekClient(
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
        private dev.slsk.messaging.messages.OutgoingMessage message;
        private CancellationSignal token;
        private int writeCount;
        private CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        private RuntimeException synchronousFailure;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("writeAsync")
                    && arguments.length == 2
                    && arguments[0] instanceof dev.slsk.messaging.messages.OutgoingMessage outgoing) {
                writeCount++;
                message = outgoing;
                token = (CancellationSignal) arguments[1];
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
        private CancellationSignal token;
        private int argumentCount;
        private int registrations;
        private CompletableFuture<?> result = CompletableFuture.completedFuture(null);
        private RuntimeException synchronousFailure;
        private final Waiter proxy = (Waiter)
                Proxy.newProxyInstance(Waiter.class.getClassLoader(), new Class<?>[] {Waiter.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("waitAsync") && (arguments.length == 3 || arguments.length == 4)) {
                registrations++;
                argumentCount = arguments.length;
                key = (WaitKey) arguments[0];
                resultType = arguments.length == 4 ? (Class<?>) arguments[1] : null;
                token = (CancellationSignal) arguments[arguments.length - 1];
                if (synchronousFailure != null) {
                    throw synchronousFailure;
                }
                return result;
            }
            return defaultValue(method.getReturnType());
        }
    }
}
