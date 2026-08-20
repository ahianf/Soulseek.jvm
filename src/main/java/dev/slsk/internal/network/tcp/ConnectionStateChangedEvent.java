// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

/** Data describing a TCP connection state change. */
public record ConnectionStateChangedEvent(
        TransportConnection connection,
        TransportState previousState,
        TransportState currentState,
        String message,
        Exception exception)
        implements TransportEvent {

    /** Creates state-change data without optional details. */
    public ConnectionStateChangedEvent(TransportState previousState, TransportState currentState) {
        this(null, previousState, currentState, null, null);
    }

    /** Creates state-change data with a message. */
    public ConnectionStateChangedEvent(TransportState previousState, TransportState currentState, String message) {
        this(null, previousState, currentState, message, null);
    }

    /** Creates state-change data. */
    public ConnectionStateChangedEvent(
            TransportState previousState, TransportState currentState, String message, Exception exception) {
        this(null, previousState, currentState, message, exception);
    }
}
