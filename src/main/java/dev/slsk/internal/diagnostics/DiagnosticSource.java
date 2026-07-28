// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.diagnostics;

/** Generates diagnostic message events. */
public interface DiagnosticSource {
    /** Adds a diagnostic event listener. */
    void addDiagnosticGeneratedListener(DiagnosticEventListener listener);

    /** Removes a diagnostic event listener. */
    void removeDiagnosticGeneratedListener(DiagnosticEventListener listener);
}
