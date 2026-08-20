// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

/** Event payload emitted when the client disconnects. */
public record SoulseekClientDisconnectedEvent(String message, Throwable exception) implements SoulseekClientEvent {

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
}
