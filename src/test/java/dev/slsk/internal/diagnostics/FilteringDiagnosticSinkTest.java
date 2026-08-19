// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.diagnostics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FilteringDiagnosticSinkTest {
    @Test
    @DisplayName("Diagnostic factory retains constructor data")
    void constructs() {
        Consumer<DiagnosticEvent> handler = ignored -> {};
        FilteringDiagnosticSink factory = new FilteringDiagnosticSink(DiagnosticLevel.DEBUG, handler);

        assertEquals(DiagnosticLevel.DEBUG, factory.getMinimumLevel());
        assertSame(handler, factory.getEventHandler());
    }

    @Test
    @DisplayName("Every diagnostic method preserves level and exception")
    void raisesEveryMessageForm() {
        List<DiagnosticEvent> events = new ArrayList<>();
        FilteringDiagnosticSink factory = new FilteringDiagnosticSink(DiagnosticLevel.TRACE, events::add);
        RuntimeException exception = new RuntimeException("broken");

        factory.trace("trace");
        factory.trace("trace-ex", exception);
        factory.debug("debug");
        factory.debug("debug-ex", exception);
        factory.info("info");
        factory.warning("warning");
        factory.warning("warning-ex", exception);

        assertEquals(7, events.size());
        assertEvent(events.get(0), DiagnosticLevel.TRACE, "trace", null);
        assertEvent(events.get(1), DiagnosticLevel.TRACE, "trace-ex", exception);
        assertEvent(events.get(2), DiagnosticLevel.DEBUG, "debug", null);
        assertEvent(events.get(3), DiagnosticLevel.DEBUG, "debug-ex", exception);
        assertEvent(events.get(4), DiagnosticLevel.INFO, "info", null);
        assertEvent(events.get(5), DiagnosticLevel.WARNING, "warning", null);
        assertEvent(events.get(6), DiagnosticLevel.WARNING, "warning-ex", exception);
    }

    @ParameterizedTest(name = "{0} retains {1} levels")
    @MethodSource("filterCases")
    @DisplayName("Minimum level filters source ordering")
    void filtersByMinimumLevel(DiagnosticLevel minimum, int count) {
        List<DiagnosticEvent> events = new ArrayList<>();
        FilteringDiagnosticSink factory = new FilteringDiagnosticSink(minimum, events::add);

        factory.trace("message");
        factory.debug("message");
        factory.info("message");
        factory.warning("message");

        assertEquals(count, events.size());
        assertTrue(events.stream().allMatch(event -> event.getLevel().getValue() <= minimum.getValue()));
    }

    static Stream<Arguments> filterCases() {
        return Stream.of(
                Arguments.of(DiagnosticLevel.NONE, 0),
                Arguments.of(DiagnosticLevel.WARNING, 1),
                Arguments.of(DiagnosticLevel.INFO, 2),
                Arguments.of(DiagnosticLevel.DEBUG, 3),
                Arguments.of(DiagnosticLevel.TRACE, 4));
    }

    private static void assertEvent(DiagnosticEvent event, DiagnosticLevel level, String message, Throwable exception) {
        assertEquals(level, event.getLevel());
        assertEquals(FilteringDiagnosticSinkTest.class.getName(), event.getSource());
        assertEquals(message, event.getMessage());
        assertSame(exception, event.getException());
        assertEquals(exception != null, event.isIncludesException());
        if (exception == null) {
            assertFalse(event.isIncludesException());
            assertNull(event.getException());
        } else {
            assertTrue(event.isIncludesException());
        }
    }
}
