// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.connection.SoulseekClientState;
import java.util.Objects;

/** Event payload emitted by a client-state change. */
public record SoulseekClientStateChangedEvent(
        SoulseekClientState previousState, SoulseekClientState state, String message, Throwable exception)
        implements SoulseekClientEvent {

    /**
     * Creates state-change event payload without a message or exception.
     *
     * @param previousState the previous client state
     * @param state the current client state
     */
    public SoulseekClientStateChangedEvent(SoulseekClientState previousState, SoulseekClientState state) {
        this(previousState, state, null, null);
    }

    /**
     * Creates state-change event payload without an exception.
     *
     * @param previousState the previous client state
     * @param state the current client state
     * @param message the associated message
     */
    public SoulseekClientStateChangedEvent(
            SoulseekClientState previousState, SoulseekClientState state, String message) {
        this(previousState, state, message, null);
    }

    /**
     * Creates state-change event payload.
     *
     * @param previousState the previous client state
     * @param state the current client state
     * @param message the associated message
     * @param exception the associated exception
     */
    public SoulseekClientStateChangedEvent {
        Objects.requireNonNull(previousState, "previousState");
        Objects.requireNonNull(state, "state");
    }
}
