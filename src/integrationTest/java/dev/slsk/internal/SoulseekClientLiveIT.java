// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.slsk.internal.diagnostics.DiagnosticLevel;
import dev.slsk.internal.events.SoulseekClientStateChangedEvent;
import dev.slsk.internal.options.SoulseekClientOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class SoulseekClientLiveIT {
    @Test
    @DisplayName("Client connects")
    void clientConnects() {
        LiveIntegrationSettings.Credentials credentials = LiveIntegrationSettings.requireCredentials();
        try (SoulseekClient client = SoulseekClient.create(credentials.minorVersion())) {
            assertDoesNotThrow(() -> client.connect(credentials.username(), credentials.password()));
            assertEquals(SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGED_IN), client.getState());
        }
    }

    @Test
    @DisplayName("Client connect raises StateChanged event")
    void clientConnectRaisesStateChangedEvent() {
        LiveIntegrationSettings.Credentials credentials = LiveIntegrationSettings.requireCredentials();
        try (SoulseekClient client = SoulseekClient.create(credentials.minorVersion())) {
            List<SoulseekClientStateChangedEvent> events = new ArrayList<>();
            client.addStateChangedListener((sender, event) -> events.add(event));

            assertDoesNotThrow(() -> client.connect(credentials.username(), credentials.password()));

            assertEquals(4, events.size());
            assertEquals(SoulseekClientState.CONNECTING, events.get(0).getState());
            assertEquals(SoulseekClientState.CONNECTED, events.get(1).getState());
            assertEquals(
                    SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGING_IN),
                    events.get(2).getState());
            assertEquals(
                    SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGED_IN),
                    events.get(3).getState());
        }
    }

    @Test
    @DisplayName("Client disconnects")
    void clientDisconnects() {
        LiveIntegrationSettings.Credentials credentials = LiveIntegrationSettings.requireCredentials();
        try (SoulseekClient client = SoulseekClient.create(credentials.minorVersion())) {
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
        try (SoulseekClient client = SoulseekClient.create(credentials.minorVersion())) {
            client.connect(credentials.username(), credentials.password());
            client.addStateChangedListener((sender, eventData) -> event.set(eventData));

            assertDoesNotThrow(() -> client.disconnect());

            assertEquals(SoulseekClientState.DISCONNECTED, client.getState());
            assertEquals(SoulseekClientState.DISCONNECTED, event.get().getState());
        }
    }

    @Test
    @DisplayName("GetNextToken returns sequential tokens")
    void getNextTokenReturnsSequentialTokens() {
        try (SoulseekClient client = SoulseekClient.create(101)) {
            int first = client.getNextToken();
            int second = client.getNextToken();

            assertEquals(first + 1, second);
        }
    }

    @Test
    @DisplayName("GetNextToken rolls over at int.MaxValue")
    void getNextTokenRollsOverAtIntMaxValue() {
        SoulseekClientOptions options = optionsStartingAtMaximumToken();
        try (SoulseekClient client = SoulseekClient.create(101, options)) {
            int first = client.getNextToken();
            int second = client.getNextToken();

            assertEquals(Integer.MAX_VALUE, first);
            assertEquals(0, second);
        }
    }

    private static SoulseekClientOptions optionsStartingAtMaximumToken() {
        return new SoulseekClientOptions(
                true,
                null,
                SoulseekClientOptions.DEFAULT_LISTEN_PORT,
                true,
                true,
                SoulseekClientOptions.DEFAULT_DISTRIBUTED_CHILD_LIMIT,
                SoulseekClientOptions.DEFAULT_MAXIMUM_CONCURRENT_SEARCHES,
                SoulseekClientOptions.DEFAULT_MAXIMUM_CONCURRENT_UPLOADS,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                true,
                SoulseekClientOptions.DEFAULT_MESSAGE_TIMEOUT,
                true,
                true,
                false,
                DiagnosticLevel.INFO,
                Integer.MAX_VALUE,
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
                null,
                null,
                false);
    }
}
