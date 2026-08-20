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

    private final Consumer<DiagnosticEvent> eventHandler;
    private final DiagnosticLevel minimumLevel;
    private final String source;

    /** Creates a diagnostic factory. */
    public FilteringDiagnosticSink(DiagnosticLevel minimumLevel, Consumer<DiagnosticEvent> eventHandler) {
        this(minimumLevel, eventHandler, CALLER.getCallerClass().getName());
    }

    private FilteringDiagnosticSink(
            DiagnosticLevel minimumLevel, Consumer<DiagnosticEvent> eventHandler, String source) {
        this.minimumLevel = Objects.requireNonNull(minimumLevel, "minimumLevel");
        this.eventHandler = Objects.requireNonNull(eventHandler, "eventHandler");
        this.source = Objects.requireNonNull(source, "source");
    }

    @Override
    public void trace(String message) {
        if (enabled(DiagnosticLevel.TRACE)) {
            publishEvent(DiagnosticLevel.TRACE, message, null);
        }
    }

    @Override
    public void trace(String message, Throwable exception) {
        if (enabled(DiagnosticLevel.TRACE)) {
            publishEvent(DiagnosticLevel.TRACE, message, exception);
        }
    }

    @Override
    public void trace(Supplier<String> message) {
        if (enabled(DiagnosticLevel.TRACE)) {
            publishEvent(DiagnosticLevel.TRACE, message.get(), null);
        }
    }

    @Override
    public void trace(Supplier<String> message, Throwable exception) {
        if (enabled(DiagnosticLevel.TRACE)) {
            publishEvent(DiagnosticLevel.TRACE, message.get(), exception);
        }
    }

    @Override
    public void debug(String message) {
        if (enabled(DiagnosticLevel.DEBUG)) {
            publishEvent(DiagnosticLevel.DEBUG, message, null);
        }
    }

    @Override
    public void debug(String message, Throwable exception) {
        if (enabled(DiagnosticLevel.DEBUG)) {
            publishEvent(DiagnosticLevel.DEBUG, message, exception);
        }
    }

    @Override
    public void debug(Supplier<String> message) {
        if (enabled(DiagnosticLevel.DEBUG)) {
            publishEvent(DiagnosticLevel.DEBUG, message.get(), null);
        }
    }

    @Override
    public void debug(Supplier<String> message, Throwable exception) {
        if (enabled(DiagnosticLevel.DEBUG)) {
            publishEvent(DiagnosticLevel.DEBUG, message.get(), exception);
        }
    }

    @Override
    public void info(String message) {
        if (enabled(DiagnosticLevel.INFO)) {
            publishEvent(DiagnosticLevel.INFO, message, null);
        }
    }

    @Override
    public void info(Supplier<String> message) {
        if (enabled(DiagnosticLevel.INFO)) {
            publishEvent(DiagnosticLevel.INFO, message.get(), null);
        }
    }

    @Override
    public void warning(String message) {
        if (enabled(DiagnosticLevel.WARNING)) {
            publishEvent(DiagnosticLevel.WARNING, message, null);
        }
    }

    @Override
    public void warning(String message, Throwable exception) {
        if (enabled(DiagnosticLevel.WARNING)) {
            publishEvent(DiagnosticLevel.WARNING, message, exception);
        }
    }

    @Override
    public void warning(Supplier<String> message) {
        if (enabled(DiagnosticLevel.WARNING)) {
            publishEvent(DiagnosticLevel.WARNING, message.get(), null);
        }
    }

    @Override
    public void warning(Supplier<String> message, Throwable exception) {
        if (enabled(DiagnosticLevel.WARNING)) {
            publishEvent(DiagnosticLevel.WARNING, message.get(), exception);
        }
    }

    public DiagnosticSink forSource(Class<?> source) {
        return new FilteringDiagnosticSink(minimumLevel, eventHandler, source.getName());
    }

    DiagnosticLevel getMinimumLevel() {
        return minimumLevel;
    }

    Consumer<DiagnosticEvent> getEventHandler() {
        return eventHandler;
    }

    private boolean enabled(DiagnosticLevel level) {
        return level.ordinal() <= minimumLevel.ordinal();
    }

    private void publishEvent(DiagnosticLevel level, String message, Throwable exception) {
        eventHandler.accept(new DiagnosticEvent(level, source, message, exception));
    }
}
