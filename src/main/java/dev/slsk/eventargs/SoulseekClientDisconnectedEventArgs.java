// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

/**
 * Event arguments raised when the client disconnects.
 */
public class SoulseekClientDisconnectedEventArgs extends SoulseekClientEventArgs {
    private final Throwable exception;
    private final String message;

    /**
     * Creates disconnect event arguments without an exception.
     *
     * @param message the disconnect message
     */
    public SoulseekClientDisconnectedEventArgs(String message) {
        this(message, null);
    }

    /**
     * Creates disconnect event arguments.
     *
     * @param message the disconnect message
     * @param exception the associated exception
     */
    public SoulseekClientDisconnectedEventArgs(String message, Throwable exception) {
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
