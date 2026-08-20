// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.diagnostics;

import java.lang.StackWalker.Option;
import java.util.function.Consumer;

/** Creates filtered diagnostic messages. */
public final class FilteringDiagnosticSink implements DiagnosticSink {
    private static final StackWalker CALLER = StackWalker.getInstance(Option.RETAIN_CLASS_REFERENCE);

    private final Consumer<DiagnosticEvent> eventHandler;
    private final DiagnosticLevel minimumLevel;

    /** Creates a diagnostic factory. */
    public FilteringDiagnosticSink(DiagnosticLevel minimumLevel, Consumer<DiagnosticEvent> eventHandler) {
        this.minimumLevel = minimumLevel;
        this.eventHandler = eventHandler;
    }

    @Override
    public void trace(String message) {
        if (enabled(DiagnosticLevel.TRACE)) {
            publishEvent(
                    DiagnosticLevel.TRACE,
                    message,
                    null,
                    CALLER.getCallerClass().getName());
        }
    }

    @Override
    public void trace(String message, Throwable exception) {
        if (enabled(DiagnosticLevel.TRACE)) {
            publishEvent(
                    DiagnosticLevel.TRACE,
                    message,
                    exception,
                    CALLER.getCallerClass().getName());
        }
    }

    @Override
    public void debug(String message) {
        if (enabled(DiagnosticLevel.DEBUG)) {
            publishEvent(
                    DiagnosticLevel.DEBUG,
                    message,
                    null,
                    CALLER.getCallerClass().getName());
        }
    }

    @Override
    public void debug(String message, Throwable exception) {
        if (enabled(DiagnosticLevel.DEBUG)) {
            publishEvent(
                    DiagnosticLevel.DEBUG,
                    message,
                    exception,
                    CALLER.getCallerClass().getName());
        }
    }

    @Override
    public void info(String message) {
        if (enabled(DiagnosticLevel.INFO)) {
            publishEvent(
                    DiagnosticLevel.INFO, message, null, CALLER.getCallerClass().getName());
        }
    }

    @Override
    public void warning(String message) {
        if (enabled(DiagnosticLevel.WARNING)) {
            publishEvent(
                    DiagnosticLevel.WARNING,
                    message,
                    null,
                    CALLER.getCallerClass().getName());
        }
    }

    @Override
    public void warning(String message, Throwable exception) {
        if (enabled(DiagnosticLevel.WARNING)) {
            publishEvent(
                    DiagnosticLevel.WARNING,
                    message,
                    exception,
                    CALLER.getCallerClass().getName());
        }
    }

    DiagnosticLevel getMinimumLevel() {
        return minimumLevel;
    }

    Consumer<DiagnosticEvent> getEventHandler() {
        return eventHandler;
    }

    private boolean enabled(DiagnosticLevel level) {
        return level.getValue() <= minimumLevel.getValue();
    }

    private void publishEvent(DiagnosticLevel level, String message, Throwable exception, String source) {
        eventHandler.accept(new DiagnosticEvent(level, source, message, exception));
    }
}
