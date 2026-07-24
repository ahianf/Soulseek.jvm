// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.diagnostics;

/** Handles an internally generated diagnostic message. */
@FunctionalInterface
public interface DiagnosticEventListener {
    /** Handles diagnostic event data. */
    void handle(IDiagnosticGenerator sender, DiagnosticEventArgs eventArgs);
}
