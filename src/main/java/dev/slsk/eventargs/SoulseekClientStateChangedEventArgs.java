// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import dev.slsk.SoulseekClientStates;
import java.util.Objects;

/**
 * Event arguments raised by a client-state change.
 */
public class SoulseekClientStateChangedEventArgs extends SoulseekClientEventArgs {
    private final Throwable exception;
    private final String message;
    private final SoulseekClientStates previousState;
    private final SoulseekClientStates state;

    /**
     * Creates state-change event arguments without a message or exception.
     *
     * @param previousState the previous client state
     * @param state the current client state
     */
    public SoulseekClientStateChangedEventArgs(SoulseekClientStates previousState, SoulseekClientStates state) {
        this(previousState, state, null, null);
    }

    /**
     * Creates state-change event arguments without an exception.
     *
     * @param previousState the previous client state
     * @param state the current client state
     * @param message the associated message
     */
    public SoulseekClientStateChangedEventArgs(
            SoulseekClientStates previousState, SoulseekClientStates state, String message) {
        this(previousState, state, message, null);
    }

    /**
     * Creates state-change event arguments.
     *
     * @param previousState the previous client state
     * @param state the current client state
     * @param message the associated message
     * @param exception the associated exception
     */
    public SoulseekClientStateChangedEventArgs(
            SoulseekClientStates previousState, SoulseekClientStates state, String message, Throwable exception) {
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
    public final SoulseekClientStates getPreviousState() {
        return previousState;
    }

    /**
     * Returns the current client state.
     *
     * @return the current state
     */
    public final SoulseekClientStates getState() {
        return state;
    }
}
