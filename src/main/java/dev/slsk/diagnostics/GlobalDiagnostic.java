// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.diagnostics;

/**
 * A statically accessible diagnostic factory for dependency-hostile paths.
 */
public final class GlobalDiagnostic {
    private static volatile DiagnosticSink factory;

    private GlobalDiagnostic() {}

    /** Sets or clears the process-wide diagnostic factory. */
    public static void init(DiagnosticSink value) {
        factory = value;
    }

    public static void trace(String message) {
        DiagnosticSink current = factory;
        if (current != null) {
            current.trace(message);
        }
    }

    public static void trace(String message, Throwable exception) {
        DiagnosticSink current = factory;
        if (current != null) {
            current.trace(message, exception);
        }
    }

    public static void debug(String message) {
        DiagnosticSink current = factory;
        if (current != null) {
            current.debug(message);
        }
    }

    public static void debug(String message, Throwable exception) {
        DiagnosticSink current = factory;
        if (current != null) {
            current.debug(message, exception);
        }
    }

    public static void info(String message) {
        DiagnosticSink current = factory;
        if (current != null) {
            current.info(message);
        }
    }

    public static void warning(String message) {
        DiagnosticSink current = factory;
        if (current != null) {
            current.warning(message);
        }
    }

    public static void warning(String message, Throwable exception) {
        DiagnosticSink current = factory;
        if (current != null) {
            current.warning(message, exception);
        }
    }
}
