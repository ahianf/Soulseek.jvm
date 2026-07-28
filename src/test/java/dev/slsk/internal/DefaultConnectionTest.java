// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.Attachment;
import dev.slsk.CancellationSignal;
import dev.slsk.ConnectionState;
import dev.slsk.ServerAddress;
import dev.slsk.Soulseek;
import dev.slsk.events.ConnectionEvent;
import dev.slsk.internal.options.SoulseekClientOptions;
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
            assertThrows(NullPointerException.class, () -> slsk.connection().connect(null));
            assertThrows(NullPointerException.class, () -> slsk.connection().connect(ServerAddress.soulseek(), null));
            assertThrows(NullPointerException.class, () -> slsk.connection().connect(null, CancellationSignal.none()));
            assertThrows(NullPointerException.class, () -> slsk.connection().ping(null));
        }
    }

    @Test
    @DisplayName("connecting without a server refuses rather than hanging")
    void connectingWhileNotConfiguredFails() {
        try (Soulseek slsk = client()) {
            // The point is that it terminates and reports, not what it reports:
            // there is no server to reach in a unit test.
            assertThrows(Exception.class, () -> slsk.connection().connect(CancellationSignal.none()));
        }
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
