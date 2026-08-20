// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.diagnostics;

/**
 * Diagnostic message level.
 */
public enum DiagnosticLevel {
    /** No diagnostic messages. */
    NONE,

    /** Warning messages. */
    WARNING,

    /** Informational messages. */
    INFO,

    /** Debug messages. */
    DEBUG,

    /** Trace messages. */
    TRACE
}
