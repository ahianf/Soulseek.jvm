// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

/** Handles messages received from the server connection. */
public interface ServerMessageHandler extends MessageHandler {
    <T> void addListener(ServerMessageEvent event, ServerMessageHandlerEventListener<T> listener);

    <T> void removeListener(ServerMessageEvent event, ServerMessageHandlerEventListener<T> listener);
}
