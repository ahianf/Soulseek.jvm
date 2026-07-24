// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

/**
 * Event arguments raised when the client disconnects.
 */
public class SoulseekClientDisconnectedEvent extends SoulseekClientEvent {
    private final Throwable exception;
    private final String message;

    /**
     * Creates disconnect event payload without an exception.
     *
     * @param message the disconnect message
     */
    public SoulseekClientDisconnectedEvent(String message) {
        this(message, null);
    }

    /**
     * Creates disconnect event payload.
     *
     * @param message the disconnect message
     * @param exception the associated exception
     */
    public SoulseekClientDisconnectedEvent(String message, Throwable exception) {
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
     * Returns the disconnect message.
     *
     * @return the message
     */
    public final String getMessage() {
        return message;
    }
}
