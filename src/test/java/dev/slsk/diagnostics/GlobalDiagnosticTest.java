// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.diagnostics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class GlobalDiagnosticTest {
    @Test
    @DisplayName("Global diagnostic delegates every overload and tolerates null")
    void delegatesAndClears() {
        List<DiagnosticEventArgs> events = new ArrayList<>();
        RuntimeException exception = new RuntimeException("broken");
        GlobalDiagnostic.init(new DiagnosticFactory(DiagnosticLevel.TRACE, events::add));
        try {
            GlobalDiagnostic.trace("trace");
            GlobalDiagnostic.trace("trace-ex", exception);
            GlobalDiagnostic.debug("debug");
            GlobalDiagnostic.debug("debug-ex", exception);
            GlobalDiagnostic.info("info");
            GlobalDiagnostic.warning("warning");
            GlobalDiagnostic.warning("warning-ex", exception);

            assertEquals(7, events.size());
            assertSame(exception, events.get(1).getException());
            assertSame(exception, events.get(3).getException());
            assertSame(exception, events.get(6).getException());
        } finally {
            GlobalDiagnostic.init(null);
        }

        GlobalDiagnostic.trace("ignored");
        GlobalDiagnostic.trace("ignored", exception);
        GlobalDiagnostic.debug("ignored");
        GlobalDiagnostic.debug("ignored", exception);
        GlobalDiagnostic.info("ignored");
        GlobalDiagnostic.warning("ignored");
        GlobalDiagnostic.warning("ignored", exception);
        assertEquals(7, events.size());
    }
}
