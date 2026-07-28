// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

/** TCP connection lifecycle state. */
public enum ConnectionState {
    /** Pending initial connection. */
    PENDING(0),
    /** A connection attempt is in progress. */
    CONNECTING(1),
    /** Connected. */
    CONNECTED(2),
    /** Disconnection is in progress. */
    DISCONNECTING(3),
    /** Disconnected. */
    DISCONNECTED(4);

    private final int value;

    ConnectionState(int value) {
        this.value = value;
    }

    /** Returns the source numeric value. */
    public int getValue() {
        return value;
    }

    /** Returns the state for a source numeric value. */
    public static ConnectionState fromValue(int value) {
        for (ConnectionState state : values()) {
            if (state.value == value) {
                return state;
            }
        }
        throw new IllegalArgumentException("Unknown connection state: " + value);
    }
}
