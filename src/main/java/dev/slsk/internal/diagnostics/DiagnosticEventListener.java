// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.diagnostics;

/**
 * Handles an internally generated diagnostic message.
 *
 * <p>This used to extend the client's generic event-listener interface. The
 * client is gone and so is that interface; components still report diagnostics
 * to whoever wired them, and the {@code sender} is how a report says which one
 * it came from.
 */
@FunctionalInterface
public interface DiagnosticEventListener {
    /**
     * Handles diagnostic event data.
     *
     * @param sender the component that generated it
     * @param eventData the diagnostic
     */
    void handle(Object sender, DiagnosticEvent eventData);
}
