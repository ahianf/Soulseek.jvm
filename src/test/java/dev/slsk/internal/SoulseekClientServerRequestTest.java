// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationController;
import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.RoomJoinForbiddenException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.UserNotFoundException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.messages.CheckPrivilegesRequest;
import dev.slsk.internal.messaging.messages.GivePrivilegesCommand;
import dev.slsk.internal.messaging.messages.JoinRoomRequest;
import dev.slsk.internal.messaging.messages.LeaveRoomRequest;
import dev.slsk.internal.messaging.messages.NewPassword;
import dev.slsk.internal.messaging.messages.RoomListRequest;
import dev.slsk.internal.messaging.messages.ServerPing;
import dev.slsk.internal.messaging.messages.UserPrivilegesRequest;
import dev.slsk.internal.messaging.messages.UserStatisticsRequest;
import dev.slsk.internal.messaging.messages.UserStatusRequest;
import dev.slsk.internal.messaging.messages.WatchUserRequest;
import dev.slsk.internal.messaging.messages.WatchUserResponse;
import dev.slsk.internal.network.MessageConnection;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
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

            CompletableFuture<?> operation = inBackground(() -> client.changePassword("new password", token));

            // The call runs on another thread now; wait for it to register

            // its wait before inspecting the probe.

            waitForWait(waiter);
            waitForWrite(connection);

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
                    SoulseekClientException.class, failureOf(() -> client.changePassword("new password")));
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

            int result = client.getPrivileges(token);

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

            CompletableFuture<Long> operation = inBackground(() -> client.pingServer(token));

            // The call runs on another thread now; wait for it to register

            // its wait before inspecting the probe.

            waitForWait(waiter);
            waitForWrite(connection);

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

            client.grantUserPrivileges("alice", 7, token);

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
            assertTrue(client.getUserPrivileged("alice", token));
            assertEquals(new WaitKey(MessageCode.Server.USER_PRIVILEGES, "alice"), waiter.key);
            assertSame(Boolean.class, waiter.resultType);
            assertEquals(
                    "alice",
                    assertInstanceOf(UserPrivilegesRequest.class, connection.message)
                            .getUsername());

            UserStatistics statistics = new UserStatistics("alice", 10, 20, 30, 40);
            waiter.result = CompletableFuture.completedFuture(statistics);
            assertSame(statistics, client.getUserStatistics("alice", token));
            assertEquals(new WaitKey(MessageCode.Server.GET_USER_STATS, "alice"), waiter.key);
            assertSame(UserStatistics.class, waiter.resultType);
            assertEquals(
                    "alice",
                    assertInstanceOf(UserStatisticsRequest.class, connection.message)
                            .getUsername());

            UserStatus status = new UserStatus("alice", UserPresence.AWAY, true);
            waiter.result = CompletableFuture.completedFuture(status);
            assertSame(status, client.getUserStatus("alice", token));
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

            assertSame(data, client.watchUser("alice"));
            assertEquals(new WaitKey(MessageCode.Server.WATCH_USER, "alice"), waiter.key);
            assertSame(WatchUserResponse.class, waiter.resultType);
            assertEquals(
                    "alice",
                    assertInstanceOf(WatchUserRequest.class, connection.message).getUsername());

            waiter.result = CompletableFuture.completedFuture(new WatchUserResponse("missing", false));
            assertInstanceOf(UserNotFoundException.class, failureOf(() -> client.watchUser("missing")));
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
            assertSame(roomList, client.getRoomList(token));
            assertEquals(new WaitKey(MessageCode.Server.ROOM_LIST), waiter.key);
            assertSame(RoomList.class, waiter.resultType);
            assertInstanceOf(RoomListRequest.class, connection.message);

            RoomData roomData = new RoomData("room", List.of(), true);
            waiter.result = CompletableFuture.completedFuture(roomData);
            assertSame(roomData, client.joinRoom("room", true, token));
            assertEquals(new WaitKey(MessageCode.Server.JOIN_ROOM, "room"), waiter.key);
            assertSame(RoomData.class, waiter.resultType);
            JoinRoomRequest join = assertInstanceOf(JoinRoomRequest.class, connection.message);
            assertEquals("room", join.getRoomName());
            assertTrue(join.isPrivate());

            waiter.result = CompletableFuture.completedFuture(null);
            client.leaveRoom("room", token);
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
            assertInstanceOf(NoResponseException.class, failureOf(() -> client.joinRoom("room")));
            assertInstanceOf(NoResponseException.class, failureOf(() -> client.leaveRoom("room")));

            waiter.result = new CompletableFuture<>();
            TimeoutException writeTimeout = new TimeoutException("write timed out");
            connection.result = CompletableFuture.failedFuture(writeTimeout);
            assertSame(
                    writeTimeout,
                    assertInstanceOf(NoResponseException.class, failureOf(() -> client.joinRoom("room")))
                            .getCause());
            assertSame(
                    writeTimeout,
                    assertInstanceOf(NoResponseException.class, failureOf(() -> client.leaveRoom("room")))
                            .getCause());
        }
    }

    @Test
    void joinPreservesServerRejectionAndRoomFailuresMapCorrectly() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            RoomJoinForbiddenException forbidden = new RoomJoinForbiddenException("forbidden");
            waiter.result = CompletableFuture.failedFuture(forbidden);
            assertSame(forbidden, failureOf(() -> client.joinRoom("room")));

            RoomJoinForbiddenException synchronousForbidden = new RoomJoinForbiddenException("synchronous");
            waiter.synchronousFailure = synchronousForbidden;
            int writesBeforeFailure = connection.writeCount;
            assertSame(synchronousForbidden, failureOf(() -> client.joinRoom("room")));
            assertEquals(writesBeforeFailure, connection.writeCount);
            waiter.synchronousFailure = null;

            waiter.result = CompletableFuture.completedFuture(new RoomData("room", List.of()));
            RuntimeException expected = new RuntimeException("write failed");
            connection.synchronousFailure = expected;
            SoulseekClientException joinFailure =
                    assertInstanceOf(SoulseekClientException.class, failureOf(() -> client.joinRoom("room")));
            assertSame(expected, joinFailure.getCause());
            SoulseekClientException leaveFailure =
                    assertInstanceOf(SoulseekClientException.class, failureOf(() -> client.leaveRoom("room")));
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
            assertSame(offline, failureOf(() -> client.getUserPrivileged("alice")));
            assertSame(offline, failureOf(() -> client.getUserStatus("alice")));

            UserNotFoundException notFound = new UserNotFoundException("missing");
            waiter.result = CompletableFuture.failedFuture(notFound);
            assertSame(notFound, failureOf(() -> client.watchUser("alice")));

            RuntimeException expected = new RuntimeException("statistics failed");
            waiter.result = CompletableFuture.failedFuture(expected);
            SoulseekClientException mapped =
                    assertInstanceOf(SoulseekClientException.class, failureOf(() -> client.getUserStatistics("alice")));
            assertSame(expected, mapped.getCause());
        }
    }

    @Test
    void validatesArgumentsAndLoginStateInSourceOrder() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            for (String bad : new String[] {null, "", " ", "\t"}) {
                assertThrows(IllegalArgumentException.class, () -> client.changePassword(bad));
                assertThrows(IllegalArgumentException.class, () -> client.grantUserPrivileges(bad, 1));
            }
            assertThrows(IllegalArgumentException.class, () -> client.grantUserPrivileges("user", 0));
            assertThrows(IllegalArgumentException.class, () -> client.grantUserPrivileges("user", -1));
            for (String bad : new String[] {null, "", " ", "\t"}) {
                assertThrows(IllegalArgumentException.class, () -> client.getUserPrivileged(bad));
                assertThrows(IllegalArgumentException.class, () -> client.getUserStatistics(bad));
                assertThrows(IllegalArgumentException.class, () -> client.getUserStatus(bad));
                assertThrows(IllegalArgumentException.class, () -> client.watchUser(bad));
            }
            for (String bad : new String[] {null, "", " ", "\t"}) {
                assertThrows(IllegalArgumentException.class, () -> client.joinRoom(bad));
                assertThrows(IllegalArgumentException.class, () -> client.leaveRoom(bad));
            }

            client.setStateForTest(SoulseekClientState.DISCONNECTED);
            assertThrows(IllegalStateException.class, () -> client.changePassword("password"));
            assertThrows(IllegalStateException.class, client::getPrivileges);
            assertThrows(IllegalStateException.class, client::pingServer);
            assertThrows(IllegalStateException.class, () -> client.grantUserPrivileges("user", 1));
            assertThrows(IllegalStateException.class, () -> client.getUserPrivileged("user"));
            assertThrows(IllegalStateException.class, () -> client.getUserStatistics("user"));
            assertThrows(IllegalStateException.class, () -> client.getUserStatus("user"));
            assertThrows(IllegalStateException.class, () -> client.watchUser("user"));
            assertThrows(IllegalStateException.class, client::getRoomList);
            assertThrows(IllegalStateException.class, () -> client.joinRoom("room"));
            assertThrows(IllegalStateException.class, () -> client.leaveRoom("room"));
            assertThrows(IllegalArgumentException.class, () -> client.changePassword(null));
            assertThrows(IllegalArgumentException.class, () -> client.grantUserPrivileges(null, 0));
        }
    }

    @Test
    void mapsOrdinaryFailuresAndPreservesTimeoutAndCancellation() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            List<Operation> operations = List.of(
                    () -> client.changePassword("password"),
                    client::getPrivileges,
                    client::pingServer,
                    () -> client.grantUserPrivileges("user", 1),
                    () -> client.getUserPrivileged("user"),
                    () -> client.getUserStatistics("user"),
                    () -> client.getUserStatus("user"),
                    () -> client.watchUser("user"),
                    client::getRoomList,
                    () -> client.joinRoom("room"),
                    () -> client.leaveRoom("room"));
            for (Operation operation : operations) {
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
    void typedAndUntypedWaitFailuresUseSourceMapping() {
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            List<Operation> operations = List.of(
                    () -> client.changePassword("password"),
                    client::getPrivileges,
                    client::pingServer,
                    () -> client.getUserPrivileged("user"),
                    () -> client.getUserStatistics("user"),
                    () -> client.getUserStatus("user"),
                    () -> client.watchUser("user"),
                    client::getRoomList);
            for (Operation operation : operations) {
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
        WaiterProbe waiter = new WaiterProbe();
        ConnectionProbe connection = new ConnectionProbe();
        try (DefaultSoulseekClient client = loggedInClient(connection, waiter)) {
            RuntimeException expected = new RuntimeException("registration failed");
            waiter.synchronousFailure = expected;

            SoulseekClientException passwordFailure =
                    assertInstanceOf(SoulseekClientException.class, failureOf(() -> client.changePassword("password")));
            assertSame(expected, passwordFailure.getCause());
            SoulseekClientException pingFailure =
                    assertInstanceOf(SoulseekClientException.class, failureOf(() -> client.pingServer()));
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

    /**
     * Waits for the background caller to register its wait and issue its write.
     *
     * <p>The call runs on another thread now, so the test has to synchronise
     * with it before inspecting the probes.
     */
    private static void waitForWait(WaiterProbe waiter) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (waiter.key == null && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }

    /** Waits for the background caller's message to reach the connection probe. */
    private static void waitForWrite(ConnectionProbe connection) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (connection.message == null && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }

    /**
     * Runs a blocking client call on a virtual thread so the test can interact
     * with it while it is in flight.
     *
     * <p>The API used to hand back a future; now the caller decides whether to
     * be concurrent, and a test that wants to observe a call mid-flight is
     * exactly such a caller. The assertions around it are unchanged.
     */
    private static <T> CompletableFuture<T> inBackground(java.util.function.Supplier<T> call) {
        return CompletableFuture.supplyAsync(call, Executors.newVirtualThreadPerTaskExecutor());
    }

    /** Void-returning variant of {@link #inBackground}. */
    private static CompletableFuture<Void> inBackground(Runnable call) {
        return CompletableFuture.runAsync(call, Executors.newVirtualThreadPerTaskExecutor());
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

    /** A blocking client call under test; void now that the API is blocking. */
    @FunctionalInterface
    private interface Operation {
        void run();
    }

    private static final class ConnectionProbe {
        private dev.slsk.internal.messaging.messages.OutgoingMessage message;
        private CancellationSignal token;
        private int writeCount;
        private CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        private RuntimeException synchronousFailure;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("writeAsync")
                    && arguments.length == 2
                    && arguments[0] instanceof dev.slsk.internal.messaging.messages.OutgoingMessage outgoing) {
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
