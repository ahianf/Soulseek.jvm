// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.Username;
import dev.slsk.events.ChatEvent;
import dev.slsk.events.MeEvent;
import dev.slsk.internal.common.Usernames;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.events.PrivateMessageReceivedEvent;
import dev.slsk.internal.network.MessageConnection;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Wire-supplied names are the network's, not the caller's, and a facet must
 * survive the ones no {@link Username} can represent.
 *
 * <p>The live server includes a blank entry in its privileged-user list. Mapped
 * through {@code Username.of}, the facet listener threw on every login and the
 * whole list was lost to every consumer, forever. These tests pin the tolerant
 * behaviour: unrepresentable entries are skipped, the events survive.
 */
@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class WireUsernameToleranceTest {

    @Test
    @DisplayName("a blank entry in the privileged-user list is skipped, not fatal to the event")
    void privilegedListSurvivesABlankEntry() throws Exception {
        try (Fixture fixture = new Fixture()) {
            CountDownLatch received = new CountDownLatch(1);
            List<MeEvent.PrivilegedUserListReceived> lists = new CopyOnWriteArrayList<>();
            fixture.me.events().subscribe(MeEvent.PrivilegedUserListReceived.class, event -> {
                lists.add(event);
                received.countDown();
            });

            fixture.client.raiseEvent(EngineEvents.Kind.PRIVILEGED_USER_LIST_RECEIVED, List.of("alice", "", "bob"));

            assertTrue(received.await(5, TimeUnit.SECONDS), "the list event never arrived");
            assertEquals(
                    List.of(Username.of("alice"), Username.of("bob")),
                    lists.get(0).users(),
                    "the representable entries survive, the blank one is skipped");
        }
    }

    @Test
    @DisplayName("a private message with an unrepresentable sender is dropped with a warning, not thrown")
    void privateMessageWithBlankSenderIsDroppedNotThrown() throws Exception {
        try (Fixture fixture = new Fixture()) {
            List<ChatEvent> chat = new CopyOnWriteArrayList<>();
            fixture.chat.events().subscribe(chat::add);

            fixture.client.raiseEvent(
                    EngineEvents.Kind.PRIVATE_MESSAGE_RECEIVED,
                    new PrivateMessageReceivedEvent(7, Instant.now(), " ", "hello", false));
            // A representable message right behind it still arrives, which is
            // what proves the listener did not die on the first one.
            CountDownLatch received = new CountDownLatch(1);
            fixture.chat.events().subscribe(ChatEvent.MessageReceived.class, event -> received.countDown());
            fixture.client.raiseEvent(
                    EngineEvents.Kind.PRIVATE_MESSAGE_RECEIVED,
                    new PrivateMessageReceivedEvent(8, Instant.now(), "bob", "still here", false));

            assertTrue(received.await(5, TimeUnit.SECONDS), "the well-formed message never arrived");
            assertEquals(1, chat.size(), "the malformed message was dropped, the well-formed one delivered");
            assertTrue(
                    fixture.diagnostics.warnings.stream().anyMatch(warning -> warning.contains("not delivered")),
                    "the drop is named in the diagnostics: " + fixture.diagnostics.warnings);
        }
    }

    @Test
    void fromWireRejectsExactlyWhatUsernameRejects() {
        assertNull(Usernames.fromWire(null));
        assertNull(Usernames.fromWire(""));
        assertNull(Usernames.fromWire("  \t"));
        assertNull(Usernames.fromWire("with\u0007control"));
        assertEquals(Username.of("alice"), Usernames.fromWire("alice"));
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

    /** A me and chat facet over an engine whose server connection is a stub. */
    private static final class Fixture implements AutoCloseable {

        private final DiagnosticProbe diagnostics = new DiagnosticProbe();
        private final SoulseekEngine client;
        private final DefaultMe me;
        private final DefaultChat chat;

        private Fixture() {
            MessageConnection connection = (MessageConnection) Proxy.newProxyInstance(
                    MessageConnection.class.getClassLoader(),
                    new Class<?>[] {MessageConnection.class},
                    (proxy, method, arguments) -> defaultValue(method.getReturnType()));
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
                    null,
                    null,
                    diagnostics.proxy,
                    null,
                    null,
                    null);
            client.setStateForTest(SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGED_IN));
            me = new DefaultMe(client, Username.of("me"), new EventBus<>("me", diagnostics.proxy), diagnostics.proxy);
            chat = new DefaultChat(client, new EventBus<>("chat", diagnostics.proxy), diagnostics.proxy);
        }

        @Override
        public void close() {
            client.close();
        }
    }

    private static final class DiagnosticProbe {
        private final List<String> warnings = new CopyOnWriteArrayList<>();
        private final DiagnosticSink proxy = (DiagnosticSink) Proxy.newProxyInstance(
                DiagnosticSink.class.getClassLoader(), new Class<?>[] {DiagnosticSink.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("warning") && arguments != null && arguments[0] instanceof String message) {
                warnings.add(message);
            }
            return defaultValue(method.getReturnType());
        }
    }
}
