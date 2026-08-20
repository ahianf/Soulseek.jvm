// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.events.UserEvent;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.events.EventBus;
import dev.slsk.internal.messaging.messages.WatchUserResponse;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.user.UserPresence;
import dev.slsk.user.Username;
import dev.slsk.user.Watch;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The point of the server-side watch subscription is the updates. The engine
 * kinds carrying them — USER_STATUS_CHANGED, USER_CANNOT_CONNECT — had no
 * facet subscriber at all, so {@code Watch.status()} returned the login-time
 * status forever and two public event types were never published.
 */
@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DefaultUsersStatusTest {

    private static final Username BOB = Username.of("bob");

    @Test
    @DisplayName("a status change reaches the watch snapshot and the event stream")
    void statusChangeUpdatesTheWatchAndPublishes() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Watch watch = fixture.users.watch(BOB);
            assertEquals(UserPresence.OFFLINE, watch.status().presence(), "the fixture's watch answer");

            CountDownLatch received = new CountDownLatch(1);
            List<UserEvent.StatusChanged> changes = new CopyOnWriteArrayList<>();
            fixture.users.events().subscribe(UserEvent.StatusChanged.class, event -> {
                changes.add(event);
                received.countDown();
            });

            fixture.client.publishEvent(
                    EngineEvents.Kind.USER_STATUS_CHANGED,
                    new dev.slsk.internal.user.UserStatus("bob", dev.slsk.internal.user.UserPresence.ONLINE, true));

            assertTrue(received.await(5, TimeUnit.SECONDS), "the status change was never published");
            assertEquals(UserPresence.ONLINE, watch.status().presence(), "the watch snapshot moved");
            assertTrue(watch.status().privileged());
            assertEquals(UserPresence.OFFLINE, changes.get(0).from().presence(), "the transition names both ends");
            assertEquals(UserPresence.ONLINE, changes.get(0).to().presence());
        }
    }

    @Test
    @DisplayName("a cannot-connect report is published for the user it names")
    void cannotConnectIsPublished() throws Exception {
        try (Fixture fixture = new Fixture()) {
            CountDownLatch received = new CountDownLatch(1);
            List<UserEvent.CannotConnect> reports = new CopyOnWriteArrayList<>();
            fixture.users.events().subscribe(UserEvent.CannotConnect.class, event -> {
                reports.add(event);
                received.countDown();
            });

            fixture.client.publishEvent(
                    EngineEvents.Kind.USER_CANNOT_CONNECT,
                    new dev.slsk.internal.events.UserCannotConnectEvent(42, "bob"));

            assertTrue(received.await(5, TimeUnit.SECONDS), "the report was never published");
            assertEquals(BOB, reports.get(0).user());
        }
    }

    private static Object defaultValue(Class<?> type) {
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

    /**
     * A users facet over an engine whose server connection is a stub and whose
     * waiter answers every watch with an offline user.
     */
    private static final class Fixture implements AutoCloseable {

        private final DiagnosticSink diagnostics = (DiagnosticSink) Proxy.newProxyInstance(
                DiagnosticSink.class.getClassLoader(),
                new Class<?>[] {DiagnosticSink.class},
                (proxy, method, arguments) -> defaultValue(method.getReturnType()));
        private final SoulseekEngine client;
        private final DefaultUsers users;

        private Fixture() {
            MessageConnection connection = (MessageConnection) Proxy.newProxyInstance(
                    MessageConnection.class.getClassLoader(),
                    new Class<?>[] {MessageConnection.class},
                    (proxy, method, arguments) -> defaultValue(method.getReturnType()));
            Waiter waiter = (Waiter) Proxy.newProxyInstance(
                    Waiter.class.getClassLoader(), new Class<?>[] {Waiter.class}, this::answerWatch);
            client = new SoulseekEngine(
                    9999,
                    null,
                    connection,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    waiter,
                    null,
                    diagnostics,
                    null,
                    null,
                    null);
            client.setStateForTest(SoulseekClientState.LOGGED_IN);
            users = new DefaultUsers(client, new EventBus<>("users", diagnostics), diagnostics);
        }

        private Object answerWatch(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("register") && arguments.length == 4) {
                return (Wait<Object>) () -> new WatchUserResponse("bob", true, null);
            }
            return defaultValue(method.getReturnType());
        }

        @Override
        public void close() {
            client.close();
        }
    }
}
