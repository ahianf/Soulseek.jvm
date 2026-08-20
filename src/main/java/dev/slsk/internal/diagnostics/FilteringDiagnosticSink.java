// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.diagnostics;

import java.lang.StackWalker.Option;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Creates filtered diagnostic messages. */
public final class FilteringDiagnosticSink implements DiagnosticSink {
    private static final StackWalker CALLER = StackWalker.getInstance(Option.RETAIN_CLASS_REFERENCE);

    private final Consumer<DiagnosticMessage> eventHandler;
    private final DiagnosticSeverity minimumLevel;
    private final String source;

    /** Creates a diagnostic factory. */
    public FilteringDiagnosticSink(DiagnosticSeverity minimumLevel, Consumer<DiagnosticMessage> eventHandler) {
        this(minimumLevel, eventHandler, CALLER.getCallerClass().getName());
    }

    private FilteringDiagnosticSink(
            DiagnosticSeverity minimumLevel, Consumer<DiagnosticMessage> eventHandler, String source) {
        this.minimumLevel = Objects.requireNonNull(minimumLevel, "minimumLevel");
        this.eventHandler = Objects.requireNonNull(eventHandler, "eventHandler");
        this.source = Objects.requireNonNull(source, "source");
    }

    @Override
    public void trace(String message) {
        if (enabled(DiagnosticSeverity.TRACE)) {
            publishEvent(DiagnosticSeverity.TRACE, message, null);
        }
    }

    @Override
    public void trace(String message, Throwable exception) {
        if (enabled(DiagnosticSeverity.TRACE)) {
            publishEvent(DiagnosticSeverity.TRACE, message, exception);
        }
    }

    @Override
    public void trace(Supplier<String> message) {
        if (enabled(DiagnosticSeverity.TRACE)) {
            publishEvent(DiagnosticSeverity.TRACE, message.get(), null);
        }
    }

    @Override
    public void trace(Supplier<String> message, Throwable exception) {
        if (enabled(DiagnosticSeverity.TRACE)) {
            publishEvent(DiagnosticSeverity.TRACE, message.get(), exception);
        }
    }

    @Override
    public void debug(String message) {
        if (enabled(DiagnosticSeverity.DEBUG)) {
            publishEvent(DiagnosticSeverity.DEBUG, message, null);
        }
    }

    @Override
    public void debug(String message, Throwable exception) {
        if (enabled(DiagnosticSeverity.DEBUG)) {
            publishEvent(DiagnosticSeverity.DEBUG, message, exception);
        }
    }

    @Override
    public void debug(Supplier<String> message) {
        if (enabled(DiagnosticSeverity.DEBUG)) {
            publishEvent(DiagnosticSeverity.DEBUG, message.get(), null);
        }
    }

    @Override
    public void debug(Supplier<String> message, Throwable exception) {
        if (enabled(DiagnosticSeverity.DEBUG)) {
            publishEvent(DiagnosticSeverity.DEBUG, message.get(), exception);
        }
    }

    @Override
    public void info(String message) {
        if (enabled(DiagnosticSeverity.INFO)) {
            publishEvent(DiagnosticSeverity.INFO, message, null);
        }
    }

    @Override
    public void info(Supplier<String> message) {
        if (enabled(DiagnosticSeverity.INFO)) {
            publishEvent(DiagnosticSeverity.INFO, message.get(), null);
        }
    }

    @Override
    public void warning(String message) {
        if (enabled(DiagnosticSeverity.WARNING)) {
            publishEvent(DiagnosticSeverity.WARNING, message, null);
        }
    }

    @Override
    public void warning(String message, Throwable exception) {
        if (enabled(DiagnosticSeverity.WARNING)) {
            publishEvent(DiagnosticSeverity.WARNING, message, exception);
        }
    }

    @Override
    public void warning(Supplier<String> message) {
        if (enabled(DiagnosticSeverity.WARNING)) {
            publishEvent(DiagnosticSeverity.WARNING, message.get(), null);
        }
    }

    @Override
    public void warning(Supplier<String> message, Throwable exception) {
        if (enabled(DiagnosticSeverity.WARNING)) {
            publishEvent(DiagnosticSeverity.WARNING, message.get(), exception);
        }
    }

    public DiagnosticSink forSource(Class<?> source) {
        return new FilteringDiagnosticSink(minimumLevel, eventHandler, source.getName());
    }

    DiagnosticSeverity getMinimumLevel() {
        return minimumLevel;
    }

    Consumer<DiagnosticMessage> getEventHandler() {
        return eventHandler;
    }

    private boolean enabled(DiagnosticSeverity level) {
        return level.ordinal() <= minimumLevel.ordinal();
    }

    private void publishEvent(DiagnosticSeverity level, String message, Throwable exception) {
        eventHandler.accept(new DiagnosticMessage(level, source, message, exception));
    }
}
