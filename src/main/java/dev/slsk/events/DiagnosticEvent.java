// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import dev.slsk.DiagnosticLevel;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Something the library wants to tell you about its own operation.
 *
 * <p>This is the channel a contained listener fault is reported on: when a
 * consumer's own event listener throws, the library records it here at {@link
 * DiagnosticLevel#WARNING} rather than letting it reach a read loop.
 *
 * @param level how important it is
 * @param message what happened
 * @param exception the cause, when there was one
 * @param at when
 */
public record DiagnosticEvent(DiagnosticLevel level, String message, Optional<Throwable> exception, Instant at)
        implements SoulseekEvent {

    /** Validates and returns the event. */
    public DiagnosticEvent {
        Objects.requireNonNull(level, "level");
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(exception, "exception");
        Objects.requireNonNull(at, "at");
    }
}
