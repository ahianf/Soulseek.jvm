// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

/** Handles an accepted TCP connection. */
@FunctionalInterface
public interface ListenerAcceptedEventListener {
    /** Handles a connection accepted by a listener. */
    void handle(Listener sender, Connection connection);
}
