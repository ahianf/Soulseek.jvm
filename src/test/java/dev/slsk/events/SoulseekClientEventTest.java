// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.SoulseekClientState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SoulseekClientEventTest {
    @Test
    @DisplayName("SoulseekClientStateChangedEvent instantiates with the given data")
    void stateChangedInstantiatesWithTheGivenData() {
        SoulseekClientStateChangedEvent args = new SoulseekClientStateChangedEvent(
                SoulseekClientState.CONNECTED, SoulseekClientState.LOGGED_IN, "message");

        assertEquals(SoulseekClientState.CONNECTED, args.getPreviousState());
        assertEquals(SoulseekClientState.LOGGED_IN, args.getState());
        assertEquals("message", args.getMessage());
        assertNull(args.getException());
    }

    @Test
    @DisplayName("SoulseekClientStateChangedEvent instantiates with Exception")
    void stateChangedInstantiatesWithException() {
        RuntimeException exception = new RuntimeException("failure");
        SoulseekClientStateChangedEvent args = new SoulseekClientStateChangedEvent(
                SoulseekClientState.CONNECTED, SoulseekClientState.DISCONNECTED, null, exception);

        assertSame(exception, args.getException());
        assertNull(args.getMessage());
    }

    @Test
    @DisplayName("SoulseekClientDisconnectedEvent instantiates with Exception")
    void disconnectedInstantiatesWithException() {
        RuntimeException exception = new RuntimeException("failure");
        SoulseekClientDisconnectedEvent args = new SoulseekClientDisconnectedEvent("message", exception);

        assertEquals("message", args.getMessage());
        assertSame(exception, args.getException());
    }

    @Test
    @DisplayName("Optional constructor values default to null")
    void optionalConstructorValuesDefaultToNull() {
        SoulseekClientStateChangedEvent stateArgs =
                new SoulseekClientStateChangedEvent(SoulseekClientState.NONE, SoulseekClientState.NONE);
        SoulseekClientDisconnectedEvent disconnectedArgs = new SoulseekClientDisconnectedEvent(null);

        assertNull(stateArgs.getMessage());
        assertNull(stateArgs.getException());
        assertNull(disconnectedArgs.getMessage());
        assertNull(disconnectedArgs.getException());
    }

    @Test
    @DisplayName("Rejects null state values that C# flags cannot represent")
    void rejectsNullStateValues() {
        assertThrows(
                NullPointerException.class, () -> new SoulseekClientStateChangedEvent(null, SoulseekClientState.NONE));
        assertThrows(
                NullPointerException.class, () -> new SoulseekClientStateChangedEvent(SoulseekClientState.NONE, null));
    }
}
