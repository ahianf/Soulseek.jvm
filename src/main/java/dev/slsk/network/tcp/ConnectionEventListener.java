// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

/** Handles a connection event. */
@FunctionalInterface
public interface ConnectionEventListener<T> {
    /** Handles an event raised by a connection. */
    void handle(IConnection sender, T eventArgs);
}
