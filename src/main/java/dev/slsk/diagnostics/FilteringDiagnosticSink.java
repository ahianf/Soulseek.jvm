// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.diagnostics;

import java.util.function.Consumer;

/** Creates filtered diagnostic messages. */
public final class FilteringDiagnosticSink implements DiagnosticSink {
    private final Consumer<DiagnosticEventArgs> eventHandler;
    private final DiagnosticLevel minimumLevel;

    /** Creates a diagnostic factory. */
    public FilteringDiagnosticSink(DiagnosticLevel minimumLevel, Consumer<DiagnosticEventArgs> eventHandler) {
        this.minimumLevel = minimumLevel;
        this.eventHandler = eventHandler;
    }

    @Override
    public void trace(String message) {
        raiseEvent(DiagnosticLevel.TRACE, message, null);
    }

    @Override
    public void trace(String message, Throwable exception) {
        raiseEvent(DiagnosticLevel.TRACE, message, exception);
    }

    @Override
    public void debug(String message) {
        raiseEvent(DiagnosticLevel.DEBUG, message, null);
    }

    @Override
    public void debug(String message, Throwable exception) {
        raiseEvent(DiagnosticLevel.DEBUG, message, exception);
    }

    @Override
    public void info(String message) {
        raiseEvent(DiagnosticLevel.INFO, message, null);
    }

    @Override
    public void warning(String message) {
        warning(message, null);
    }

    @Override
    public void warning(String message, Throwable exception) {
        raiseEvent(DiagnosticLevel.WARNING, message, exception);
    }

    DiagnosticLevel getMinimumLevel() {
        return minimumLevel;
    }

    Consumer<DiagnosticEventArgs> getEventHandler() {
        return eventHandler;
    }

    private void raiseEvent(DiagnosticLevel level, String message, Throwable exception) {
        if (level.getValue() <= minimumLevel.getValue()) {
            eventHandler.accept(new DiagnosticEventArgs(level, message, exception));
        }
    }
}
