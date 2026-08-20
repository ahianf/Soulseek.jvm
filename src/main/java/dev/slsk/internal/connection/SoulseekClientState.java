// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.connection;

/** The client's mutually exclusive connection lifecycle phase. */
public enum SoulseekClientState {
    /** No lifecycle phase has been assigned. */
    NONE,
    /** The client is disconnected. */
    DISCONNECTED,
    /** A connection attempt is in progress. */
    CONNECTING,
    /** The server transport is connected. */
    CONNECTED,
    /** Authentication is in progress. */
    LOGGING_IN,
    /** The server transport is connected and authenticated. */
    LOGGED_IN,
    /** Disconnection is in progress. */
    DISCONNECTING;

    /** Returns whether the server transport is currently connected. */
    public boolean isConnected() {
        return this == CONNECTED || this == LOGGING_IN || this == LOGGED_IN;
    }

    /** Returns whether the client is authenticated. */
    public boolean isLoggedIn() {
        return this == LOGGED_IN;
    }
}
