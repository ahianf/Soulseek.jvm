// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

/** Handles a message-connection event. */
@FunctionalInterface
public interface MessageConnectionEventListener<T extends MessageConnectionEvent> {
    /** Handles an event raised by a message connection. */
    void handle(MessageConnection sender, T eventData);
}
