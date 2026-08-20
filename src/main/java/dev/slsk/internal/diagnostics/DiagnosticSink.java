// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.diagnostics;

import java.util.Objects;
import java.util.function.Supplier;

/** Creates diagnostic messages. */
public interface DiagnosticSink {
    void trace(String message);

    void trace(String message, Throwable exception);

    default void trace(Supplier<String> message) {
        trace(message.get());
    }

    default void trace(Supplier<String> message, Throwable exception) {
        trace(message.get(), exception);
    }

    void debug(String message);

    void debug(String message, Throwable exception);

    default void debug(Supplier<String> message) {
        debug(message.get());
    }

    default void debug(Supplier<String> message, Throwable exception) {
        debug(message.get(), exception);
    }

    void info(String message);

    default void info(Supplier<String> message) {
        info(message.get());
    }

    void warning(String message);

    void warning(String message, Throwable exception);

    default void warning(Supplier<String> message) {
        warning(message.get());
    }

    default void warning(Supplier<String> message, Throwable exception) {
        warning(message.get(), exception);
    }

    /** Returns a view whose events identify the component that owns it. */
    default DiagnosticSink forSource(Class<?> source) {
        Objects.requireNonNull(source, "source");
        return this;
    }
}
