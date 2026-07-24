// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.handlers;

/** Handles a peer-message-handler event. */
@FunctionalInterface
public interface PeerMessageHandlerEventListener<T> {
    void handle(PeerMessageHandler sender, T eventData);
}
