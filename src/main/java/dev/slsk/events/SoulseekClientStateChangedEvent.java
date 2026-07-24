// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import dev.slsk.SoulseekClientState;
import java.util.Objects;

/**
 * Event arguments raised by a client-state change.
 */
public class SoulseekClientStateChangedEvent extends SoulseekClientEvent {
    private final Throwable exception;
    private final String message;
    private final SoulseekClientState previousState;
    private final SoulseekClientState state;

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
    public SoulseekClientStateChangedEvent(
            SoulseekClientState previousState, SoulseekClientState state, String message, Throwable exception) {
        this.previousState = Objects.requireNonNull(previousState, "previousState");
        this.state = Objects.requireNonNull(state, "state");
        this.message = message;
        this.exception = exception;
    }

    /**
     * Returns the associated exception.
     *
     * @return the exception, or {@code null}
     */
    public final Throwable getException() {
        return exception;
    }

    /**
     * Returns the associated message.
     *
     * @return the message, or {@code null}
     */
    public final String getMessage() {
        return message;
    }

    /**
     * Returns the previous client state.
     *
     * @return the previous state
     */
    public final SoulseekClientState getPreviousState() {
        return previousState;
    }

    /**
     * Returns the current client state.
     *
     * @return the current state
     */
    public final SoulseekClientState getState() {
        return state;
    }
}
