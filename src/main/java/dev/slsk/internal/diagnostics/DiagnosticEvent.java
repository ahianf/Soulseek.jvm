// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.diagnostics;

import java.time.Instant;
import java.util.Objects;

/**
 * Event payload for a diagnostic message.
 */
public class DiagnosticEvent {
    private final Throwable exception;
    private final boolean includesException;
    private final DiagnosticLevel level;
    private final String message;
    private final String source;
    private final Instant timestamp;

    /**
     * Creates diagnostic event arguments without an exception.
     *
     * @param level the diagnostic level
     * @param source the fully qualified name of the class that emitted the diagnostic
     * @param message the diagnostic message
     */
    public DiagnosticEvent(DiagnosticLevel level, String source, String message) {
        this(level, source, message, null);
    }

    /**
     * Creates diagnostic event arguments.
     *
     * @param level the diagnostic level
     * @param source the fully qualified name of the class that emitted the diagnostic
     * @param message the diagnostic message
     * @param exception the associated exception
     */
    public DiagnosticEvent(DiagnosticLevel level, String source, String message, Throwable exception) {
        this.level = Objects.requireNonNull(level, "level");
        this.source = Objects.requireNonNull(source, "source");
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
     * Returns the class that emitted the diagnostic.
     *
     * <p>The value is a fully qualified class name suitable for use as a logging
     * framework category.
     *
     * @return the fully qualified emitter class name
     */
    public final String getSource() {
        return source;
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
