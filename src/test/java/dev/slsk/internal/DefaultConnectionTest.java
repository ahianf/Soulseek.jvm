// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.Attachment;
import dev.slsk.Soulseek;
import dev.slsk.connection.ConnectionState;
import dev.slsk.connection.ServerAddress;
import dev.slsk.events.ConnectionEvent;
import dev.slsk.exceptions.ConnectionReadException;
import dev.slsk.exceptions.LoginRejectedException;
import dev.slsk.internal.events.SoulseekClientDisconnectedEvent;
import dev.slsk.internal.options.SoulseekClientOptions;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DefaultConnectionTest {

    private static Soulseek client() {
        return DefaultSoulseek.create("user", "password", 157, new SoulseekClientOptions());
    }

    @Test
    @DisplayName("a fresh client is Offline, not a bit-set the caller has to decode")
    void startsOffline() {
        try (Soulseek slsk = client()) {
            assertInstanceOf(ConnectionState.Offline.class, slsk.connection().state());
            assertFalse(slsk.connection().state().isOnline());
        }
    }

    @Test
    void reportsNoServerInfoWhileOffline() {
        try (Soulseek slsk = client()) {
            assertTrue(slsk.connection().server().isEmpty());
        }
    }

    @Test
    @DisplayName("state() is synchronous and cheap, so a cold render needs no event history")
    void stateIsReadableWithoutSubscribing() {
        try (Soulseek slsk = client()) {
            for (int i = 0; i < 100; i++) {
                assertInstanceOf(
                        ConnectionState.Offline.class, slsk.connection().state());
            }
        }
    }

    @Test
    void attachReturnsTheStateAndSubscribes() {
        try (Soulseek slsk = client()) {
            List<ConnectionEvent> events = new ArrayList<>();
            try (Attachment<ConnectionState> attached = slsk.connection().attach(events::add)) {
                assertInstanceOf(ConnectionState.Offline.class, attached.state());
            }
        }
    }

    @Test
    @DisplayName("disconnect is an idempotent intent, not an error on an idle client")
    void disconnectingWhileOfflineIsANoOp() {
        try (Soulseek slsk = client()) {
            slsk.connection().disconnect("first");
            slsk.connection().disconnect("second");
            assertInstanceOf(ConnectionState.Offline.class, slsk.connection().state());
        }
    }

    @Test
    void closeIsIdempotent() {
        Soulseek slsk = client();
        slsk.close();
        slsk.close();
    }

    @Test
    void rejectsNullArguments() {
        try (Soulseek slsk = client()) {
            assertThrows(NullPointerException.class, () -> slsk.connection().connect((java.time.Duration) null));
            assertThrows(NullPointerException.class, () -> slsk.connection().connect(ServerAddress.soulseek(), null));
            assertThrows(NullPointerException.class, () -> slsk.connection().connect((ServerAddress) null));
            assertThrows(NullPointerException.class, () -> slsk.connection().ping(null));
        }
    }

    @Test
    @DisplayName("connecting without a server refuses rather than hanging")
    void connectingWhileNotConfiguredFails() {
        try (Soulseek slsk = client()) {
            // The point is that it terminates and reports, not what it reports:
            // there is no server to reach in a unit test.
            assertThrows(Exception.class, () -> slsk.connection().connect());
        }
    }

    @Test
    @DisplayName("a connect that fails starts trying again rather than staying offline")
    void afailedConnectKeepsTrying() throws Exception {
        try (Soulseek slsk = client()) {
            ServerAddress nowhere = ServerAddress.of("127.0.0.1", closedPort());

            // The call still throws, because Connection documents that it does.
            assertThrows(Exception.class, () -> slsk.connection().connect(nowhere));

            // ...and the supervisor picks it up from there. This is the case
            // that used to cost a restart: one transient failure at startup and
            // the process stayed offline until someone noticed.
            assertTrue(
                    awaitState(slsk, ConnectionState.Reconnecting.class),
                    "a failed connect left the client offline instead of retrying");
        }
    }

    @Test
    @DisplayName("an explicit disconnect is honoured, not reconnected over the top of")
    void disconnectStopsTheRetrying() throws Exception {
        try (Soulseek slsk = client()) {
            ServerAddress nowhere = ServerAddress.of("127.0.0.1", closedPort());
            assertThrows(Exception.class, () -> slsk.connection().connect(nowhere));
            assertTrue(awaitState(slsk, ConnectionState.Reconnecting.class));

            slsk.connection().disconnect("that will do");

            // Offline immediately, and it stays that way: the consumer's intent
            // outranks the library's.
            assertInstanceOf(ConnectionState.Offline.class, slsk.connection().state());
            Thread.sleep(300);
            assertInstanceOf(ConnectionState.Offline.class, slsk.connection().state());
        }
    }

    @Test
    @DisplayName("closing the client stops the retrying")
    void closeStopsTheRetrying() throws Exception {
        Soulseek slsk = client();
        ServerAddress nowhere = ServerAddress.of("127.0.0.1", closedPort());
        assertThrows(Exception.class, () -> slsk.connection().connect(nowhere));
        assertTrue(awaitState(slsk, ConnectionState.Reconnecting.class));

        slsk.close();

        assertInstanceOf(ConnectionState.Offline.class, slsk.connection().state());
    }

    @Test
    @DisplayName("a dropped connection starts trying again on its own")
    void aDroppedConnectionReconnects() throws Exception {
        try (Soulseek slsk = client()) {
            // The production path exactly: the read loop surfaces a dead socket
            // as a disconnect nobody asked for. Reaching for the event rather
            // than a real socket is what makes this assertable offline — the
            // branch under test is the one that reads it.
            engineOf(slsk)
                    .events()
                    .raise(
                            EngineEvents.Kind.DISCONNECTED,
                            new SoulseekClientDisconnectedEvent(
                                    "Read error: Connection timed out",
                                    new ConnectionReadException("Connection timed out")));

            assertTrue(awaitState(slsk, ConnectionState.Reconnecting.class), "a dropped connection stayed offline");
        }
    }

    @Test
    @DisplayName("a login the server refused is terminal, not something to retry")
    void aRejectedLoginDoesNotReconnect() throws Exception {
        try (Soulseek slsk = client()) {
            engineOf(slsk)
                    .events()
                    .raise(
                            EngineEvents.Kind.DISCONNECTED,
                            new SoulseekClientDisconnectedEvent(
                                    "The server rejected the login", new LoginRejectedException("INVALIDPASS")));

            // A wrong password does not become right by waiting, and a client
            // that retries it forever is abuse from the server's side.
            Thread.sleep(300);
            assertInstanceOf(ConnectionState.Offline.class, slsk.connection().state());
        }
    }

    /** The engine behind a client, for a test that needs to raise its events. */
    private static SoulseekEngine engineOf(Soulseek slsk) {
        return ((DefaultSoulseek) slsk).client();
    }

    /** A port with nothing on it, so a connect is refused rather than routed. */
    private static int closedPort() throws IOException {
        try (ServerSocket probe = new ServerSocket(0)) {
            return probe.getLocalPort();
        }
    }

    /** Waits for a state, because the supervisor runs on its own thread. */
    private static boolean awaitState(Soulseek slsk, Class<? extends ConnectionState> wanted)
            throws InterruptedException {
        for (int waited = 0; waited < 5_000; waited += 20) {
            if (wanted.isInstance(slsk.connection().state())) {
                return true;
            }
            Thread.sleep(20);
        }
        return false;
    }

    @Test
    void exposesAnEventStream() {
        try (Soulseek slsk = client()) {
            List<ConnectionEvent> seen = new ArrayList<>();
            try (var subscription = slsk.connection().events().subscribe(seen::add)) {
                assertEquals(0, seen.size());
            }
        }
    }

    @Test
    @DisplayName("a switch over ConnectionState needs no default")
    void connectionStateIsExhaustivelySwitchable() {
        ConnectionState state = new ConnectionState.Offline();
        String rendered =
                switch (state) {
                    case ConnectionState.Offline ignored -> "offline";
                    case ConnectionState.Connecting connecting -> "connecting " + connecting.attempt();
                    case ConnectionState.Authenticating ignored -> "authenticating";
                    case ConnectionState.Online online -> "online since " + online.since();
                    case ConnectionState.Disconnecting ignored -> "disconnecting";
                    case ConnectionState.Reconnecting reconnecting -> "retry at " + reconnecting.nextAttemptAt();
                    case ConnectionState.Rejected rejected -> "rejected: " + rejected.reason();
                };
        assertEquals("offline", rendered);
    }
}
