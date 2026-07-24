// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.diagnostics;

/** Creates diagnostic messages. */
public interface IDiagnosticFactory {
    void trace(String message);

    void trace(String message, Throwable exception);

    void debug(String message);

    void debug(String message, Throwable exception);

    void info(String message);

    void warning(String message);

    void warning(String message, Throwable exception);
}
