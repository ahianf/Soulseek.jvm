// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

/** TCP connection lifecycle state. */
public enum ConnectionState {
    /** Pending initial connection. */
    PENDING,
    /** A connection attempt is in progress. */
    CONNECTING,
    /** Connected. */
    CONNECTED,
    /** Disconnection is in progress. */
    DISCONNECTING,
    /** Disconnected. */
    DISCONNECTED
}
