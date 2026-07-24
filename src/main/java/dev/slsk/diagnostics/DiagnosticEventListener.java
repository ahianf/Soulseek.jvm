// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.diagnostics;

import dev.slsk.SoulseekClientEventListener;

/** Handles an internally generated diagnostic message. */
@FunctionalInterface
public interface DiagnosticEventListener extends SoulseekClientEventListener<DiagnosticEventArgs> {
    /** Handles diagnostic event data. */
    @Override
    void handle(Object sender, DiagnosticEventArgs eventArgs);
}
