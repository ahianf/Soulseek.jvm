// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.SoulseekClientStates;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SoulseekClientEventArgsTest {
    @Test
    @DisplayName("SoulseekClientStateChangedEventArgs instantiates with the given data")
    void stateChangedInstantiatesWithTheGivenData() {
        SoulseekClientStateChangedEventArgs args = new SoulseekClientStateChangedEventArgs(
                SoulseekClientStates.CONNECTED, SoulseekClientStates.LOGGED_IN, "message");

        assertEquals(SoulseekClientStates.CONNECTED, args.getPreviousState());
        assertEquals(SoulseekClientStates.LOGGED_IN, args.getState());
        assertEquals("message", args.getMessage());
        assertNull(args.getException());
    }

    @Test
    @DisplayName("SoulseekClientStateChangedEventArgs instantiates with Exception")
    void stateChangedInstantiatesWithException() {
        RuntimeException exception = new RuntimeException("failure");
        SoulseekClientStateChangedEventArgs args = new SoulseekClientStateChangedEventArgs(
                SoulseekClientStates.CONNECTED, SoulseekClientStates.DISCONNECTED, null, exception);

        assertSame(exception, args.getException());
        assertNull(args.getMessage());
    }

    @Test
    @DisplayName("SoulseekClientDisconnectedEventArgs instantiates with Exception")
    void disconnectedInstantiatesWithException() {
        RuntimeException exception = new RuntimeException("failure");
        SoulseekClientDisconnectedEventArgs args = new SoulseekClientDisconnectedEventArgs("message", exception);

        assertEquals("message", args.getMessage());
        assertSame(exception, args.getException());
    }

    @Test
    @DisplayName("Optional constructor values default to null")
    void optionalConstructorValuesDefaultToNull() {
        SoulseekClientStateChangedEventArgs stateArgs =
                new SoulseekClientStateChangedEventArgs(SoulseekClientStates.NONE, SoulseekClientStates.NONE);
        SoulseekClientDisconnectedEventArgs disconnectedArgs = new SoulseekClientDisconnectedEventArgs(null);

        assertNull(stateArgs.getMessage());
        assertNull(stateArgs.getException());
        assertNull(disconnectedArgs.getMessage());
        assertNull(disconnectedArgs.getException());
    }

    @Test
    @DisplayName("Rejects null state values that C# flags cannot represent")
    void rejectsNullStateValues() {
        assertThrows(
                NullPointerException.class,
                () -> new SoulseekClientStateChangedEventArgs(null, SoulseekClientStates.NONE));
        assertThrows(
                NullPointerException.class,
                () -> new SoulseekClientStateChangedEventArgs(SoulseekClientStates.NONE, null));
    }
}
