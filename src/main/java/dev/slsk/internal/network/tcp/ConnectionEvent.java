// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

/** Base type for internal TCP connection event payloads. */
public sealed interface ConnectionEvent
        permits ConnectionDataEvent, ConnectionDisconnectedEvent, ConnectionStateChangedEvent {
    /** Returns the connection that emitted this event, when known. */
    Connection connection();
}
