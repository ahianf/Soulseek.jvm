// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.diagnostics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DiagnosticEventTest {
    @Test
    @DisplayName("DiagnosticEvent instantiates with the given data")
    void instantiatesWithTheGivenData() {
        Instant before = Instant.now();
        RuntimeException exception = new RuntimeException("failure");

        DiagnosticEvent args = new DiagnosticEvent(DiagnosticLevel.WARNING, "message", exception);

        assertEquals(DiagnosticLevel.WARNING, args.getLevel());
        assertEquals("message", args.getMessage());
        assertSame(exception, args.getException());
        assertTrue(args.isIncludesException());
        assertFalse(args.getTimestamp().isBefore(before));
        assertFalse(args.getTimestamp().isAfter(Instant.now()));
    }

    @Test
    @DisplayName("DiagnosticEvent instantiates with null Exception given null")
    void instantiatesWithNullExceptionGivenNull() {
        DiagnosticEvent args = new DiagnosticEvent(DiagnosticLevel.INFO, null);

        assertEquals(DiagnosticLevel.INFO, args.getLevel());
        assertNull(args.getMessage());
        assertNull(args.getException());
        assertFalse(args.isIncludesException());
    }

    @Test
    @DisplayName("Rejects null level because the C# enum is non-nullable")
    void rejectsNullLevel() {
        assertThrows(NullPointerException.class, () -> new DiagnosticEvent(null, "message"));
    }
}
