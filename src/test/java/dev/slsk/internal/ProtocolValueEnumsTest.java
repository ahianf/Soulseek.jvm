// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.internal.diagnostics.DiagnosticLevel;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ProtocolValueEnumsTest {
    @ParameterizedTest(name = "{0} uses value {1}")
    @MethodSource("transferDirections")
    void preservesTransferDirectionValues(TransferDirection value, int expected) {
        assertEquals(expected, value.getValue());
        assertEquals(value, TransferDirection.fromValue(expected));
    }

    @ParameterizedTest(name = "{0} uses value {1}")
    @MethodSource("fileAttributeTypes")
    void preservesFileAttributeTypeValues(FileAttributeType value, int expected) {
        assertEquals(expected, value.getValue());
        assertEquals(value, FileAttributeType.fromValue(expected));
    }

    @ParameterizedTest(name = "{0} uses value {1}")
    @MethodSource("searchScopeTypes")
    void preservesSearchScopeTypeValues(SearchScopeType value, int expected) {
        assertEquals(expected, value.getValue());
        assertEquals(value, SearchScopeType.fromValue(expected));
    }

    @ParameterizedTest(name = "{0} uses value {1}")
    @MethodSource("userPresences")
    void preservesUserPresenceValues(UserPresence value, int expected) {
        assertEquals(expected, value.getValue());
        assertEquals(value, UserPresence.fromValue(expected));
    }

    @ParameterizedTest(name = "{0} uses value {1}")
    @MethodSource("diagnosticLevels")
    void preservesDiagnosticLevelValues(DiagnosticLevel value, int expected) {
        assertEquals(expected, value.getValue());
        assertEquals(value, DiagnosticLevel.fromValue(expected));
    }

    @Test
    @DisplayName("Rejects unknown protocol enum values")
    void rejectsUnknownValues() {
        assertThrows(IllegalArgumentException.class, () -> TransferDirection.fromValue(-1));
        assertThrows(IllegalArgumentException.class, () -> FileAttributeType.fromValue(3));
        assertThrows(IllegalArgumentException.class, () -> SearchScopeType.fromValue(4));
        assertThrows(IllegalArgumentException.class, () -> UserPresence.fromValue(3));
        assertThrows(IllegalArgumentException.class, () -> DiagnosticLevel.fromValue(5));
    }

    private static Stream<Arguments> transferDirections() {
        return Stream.of(Arguments.of(TransferDirection.DOWNLOAD, 0), Arguments.of(TransferDirection.UPLOAD, 1));
    }

    private static Stream<Arguments> fileAttributeTypes() {
        return Stream.of(
                Arguments.of(FileAttributeType.BIT_RATE, 0),
                Arguments.of(FileAttributeType.LENGTH, 1),
                Arguments.of(FileAttributeType.VARIABLE_BIT_RATE, 2),
                Arguments.of(FileAttributeType.SAMPLE_RATE, 4),
                Arguments.of(FileAttributeType.BIT_DEPTH, 5));
    }

    private static Stream<Arguments> searchScopeTypes() {
        return Stream.of(
                Arguments.of(SearchScopeType.NETWORK, 0),
                Arguments.of(SearchScopeType.USER, 1),
                Arguments.of(SearchScopeType.ROOM, 2),
                Arguments.of(SearchScopeType.WISHLIST, 3));
    }

    private static Stream<Arguments> userPresences() {
        return Stream.of(
                Arguments.of(UserPresence.OFFLINE, 0),
                Arguments.of(UserPresence.AWAY, 1),
                Arguments.of(UserPresence.ONLINE, 2));
    }

    private static Stream<Arguments> diagnosticLevels() {
        return Stream.of(
                Arguments.of(DiagnosticLevel.NONE, 0),
                Arguments.of(DiagnosticLevel.WARNING, 1),
                Arguments.of(DiagnosticLevel.INFO, 2),
                Arguments.of(DiagnosticLevel.DEBUG, 3),
                Arguments.of(DiagnosticLevel.TRACE, 4));
    }
}
