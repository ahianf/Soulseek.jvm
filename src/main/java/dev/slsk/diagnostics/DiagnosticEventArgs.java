// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.diagnostics;

import java.time.Instant;
import java.util.Objects;

/**
 * Event arguments for a diagnostic message.
 */
public class DiagnosticEventArgs {
    private final Throwable exception;
    private final boolean includesException;
    private final DiagnosticLevel level;
    private final String message;
    private final Instant timestamp;

    /**
     * Creates diagnostic event arguments without an exception.
     *
     * @param level the diagnostic level
     * @param message the diagnostic message
     */
    public DiagnosticEventArgs(DiagnosticLevel level, String message) {
        this(level, message, null);
    }

    /**
     * Creates diagnostic event arguments.
     *
     * @param level the diagnostic level
     * @param message the diagnostic message
     * @param exception the associated exception
     */
    public DiagnosticEventArgs(DiagnosticLevel level, String message, Throwable exception) {
        this.level = Objects.requireNonNull(level, "level");
        this.message = message;
        this.exception = exception;
        this.timestamp = Instant.now();
        this.includesException = exception != null;
    }

    /**
     * Returns the associated exception.
     *
     * @return the exception, or {@code null}
     */
    public final Throwable getException() {
        return exception;
    }

    /**
     * Returns whether an exception is included.
     *
     * @return whether an exception is included
     */
    public final boolean isIncludesException() {
        return includesException;
    }

    /**
     * Returns the diagnostic level.
     *
     * @return the diagnostic level
     */
    public final DiagnosticLevel getLevel() {
        return level;
    }

    /**
     * Returns the diagnostic message.
     *
     * @return the diagnostic message
     */
    public final String getMessage() {
        return message;
    }

    /**
     * Returns the UTC creation timestamp.
     *
     * @return the creation timestamp
     */
    public final Instant getTimestamp() {
        return timestamp;
    }
}
