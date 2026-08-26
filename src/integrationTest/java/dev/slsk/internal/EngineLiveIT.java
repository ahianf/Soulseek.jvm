// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.events.SoulseekClientStateChangedEvent;
import dev.slsk.internal.options.SoulseekClientOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class EngineLiveIT {
    @Test
    @DisplayName("Client connects")
    void clientConnects() {
        LiveIntegrationSettings.Credentials credentials = LiveIntegrationSettings.requireCredentials();
        try (SoulseekEngine client =
                new SoulseekEngine(credentials.minorVersion(), LiveIntegrationSettings.options())) {
            assertDoesNotThrow(() -> client.connect(credentials.username(), credentials.password()));
            assertEquals(SoulseekClientState.LOGGED_IN, client.getState());
        }
    }

    @Test
    @DisplayName("Client connect raises StateChanged event")
    void clientConnectRaisesStateChangedEvent() {
        LiveIntegrationSettings.Credentials credentials = LiveIntegrationSettings.requireCredentials();
        try (SoulseekEngine client =
                new SoulseekEngine(credentials.minorVersion(), LiveIntegrationSettings.options())) {
            List<SoulseekClientStateChangedEvent> events = new ArrayList<>();
            client.events()
                    .on(
                            Kind.STATE_CHANGED,
                            (dev.slsk.internal.events.SoulseekClientStateChangedEvent event) -> events.add(event));

            assertDoesNotThrow(() -> client.connect(credentials.username(), credentials.password()));

            assertEquals(4, events.size());
            assertEquals(SoulseekClientState.CONNECTING, events.get(0).state());
            assertEquals(SoulseekClientState.CONNECTED, events.get(1).state());
            assertEquals(SoulseekClientState.LOGGING_IN, events.get(2).state());
            assertEquals(SoulseekClientState.LOGGED_IN, events.get(3).state());
        }
    }

    @Test
    @DisplayName("Client disconnects")
    void clientDisconnects() {
        LiveIntegrationSettings.Credentials credentials = LiveIntegrationSettings.requireCredentials();
        try (SoulseekEngine client =
                new SoulseekEngine(credentials.minorVersion(), LiveIntegrationSettings.options())) {
            client.connect(credentials.username(), credentials.password());

            assertDoesNotThrow(() -> client.disconnect());
            assertEquals(SoulseekClientState.DISCONNECTED, client.getState());
        }
    }

    @Test
    @DisplayName("Client disconnect raises StateChanged event")
    void clientDisconnectRaisesStateChangedEvent() {
        LiveIntegrationSettings.Credentials credentials = LiveIntegrationSettings.requireCredentials();
        AtomicReference<SoulseekClientStateChangedEvent> event = new AtomicReference<>();
        try (SoulseekEngine client =
                new SoulseekEngine(credentials.minorVersion(), LiveIntegrationSettings.options())) {
            client.connect(credentials.username(), credentials.password());
            client.events()
                    .on(
                            Kind.STATE_CHANGED,
                            (dev.slsk.internal.events.SoulseekClientStateChangedEvent eventData) ->
                                    event.set(eventData));

            assertDoesNotThrow(() -> client.disconnect());

            assertEquals(SoulseekClientState.DISCONNECTED, client.getState());
            assertEquals(SoulseekClientState.DISCONNECTED, event.get().state());
        }
    }

    @Test
    @DisplayName("GetNextToken returns sequential tokens")
    void getNextTokenReturnsSequentialTokens() {
        try (SoulseekEngine client = new SoulseekEngine(101)) {
            int first = client.getNextToken();
            int second = client.getNextToken();

            assertEquals(first + 1, second);
        }
    }

    @Test
    @DisplayName("GetNextToken rolls over at int.MaxValue")
    void getNextTokenRollsOverAtIntMaxValue() {
        SoulseekClientOptions options = optionsStartingAtMaximumToken();
        try (SoulseekEngine client = new SoulseekEngine(101, options)) {
            int first = client.getNextToken();
            int second = client.getNextToken();

            assertEquals(Integer.MAX_VALUE, first);
            assertEquals(0, second);
        }
    }

    private static SoulseekClientOptions optionsStartingAtMaximumToken() {
        return SoulseekClientOptions.builder()
                .startingToken(Integer.MAX_VALUE)
                .build();
    }
}
