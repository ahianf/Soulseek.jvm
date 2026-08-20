// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.diagnostics;

import java.time.Instant;
import java.util.Objects;

/** Event payload for a diagnostic message. */
public record DiagnosticEvent(
        DiagnosticLevel level, String source, String message, Throwable exception, Instant timestamp) {

    public DiagnosticEvent {
        level = Objects.requireNonNull(level, "level");
        source = Objects.requireNonNull(source, "source");
    }

    public DiagnosticEvent(DiagnosticLevel level, String source, String message) {
        this(level, source, message, null);
    }

    public DiagnosticEvent(DiagnosticLevel level, String source, String message, Throwable exception) {
        this(level, source, message, exception, Instant.now());
    }

    public boolean includesException() {
        return exception != null;
    }
}
