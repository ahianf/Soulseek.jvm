// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.search.SearchScopeType;
import dev.slsk.internal.share.WireFileAttribute;
import dev.slsk.internal.transfer.TransferDirection;
import dev.slsk.internal.user.WireUserPresence;
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
    void preservesFileAttributeTypeValues(WireFileAttribute value, int expected) {
        assertEquals(expected, value.getValue());
        assertEquals(value, WireFileAttribute.fromValue(expected));
        assertEquals(value, WireFileAttribute.tryFromValue(expected).orElseThrow());
    }

    @ParameterizedTest(name = "{0} uses value {1}")
    @MethodSource("searchScopeTypes")
    void preservesSearchScopeTypeValues(SearchScopeType value, int expected) {
        assertEquals(expected, value.getValue());
        assertEquals(value, SearchScopeType.fromValue(expected));
    }

    @ParameterizedTest(name = "{0} uses value {1}")
    @MethodSource("userPresences")
    void preservesUserPresenceValues(WireUserPresence value, int expected) {
        assertEquals(expected, value.getValue());
        assertEquals(value, WireUserPresence.fromValue(expected));
    }

    @Test
    @DisplayName("Rejects unknown protocol enum values")
    void rejectsUnknownValues() {
        assertThrows(IllegalArgumentException.class, () -> TransferDirection.fromValue(-1));
        assertThrows(IllegalArgumentException.class, () -> WireFileAttribute.fromValue(3));
        assertTrue(WireFileAttribute.tryFromValue(3).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> SearchScopeType.fromValue(4));
        assertThrows(IllegalArgumentException.class, () -> WireUserPresence.fromValue(3));
    }

    private static Stream<Arguments> transferDirections() {
        return Stream.of(Arguments.of(TransferDirection.DOWNLOAD, 0), Arguments.of(TransferDirection.UPLOAD, 1));
    }

    private static Stream<Arguments> fileAttributeTypes() {
        return Stream.of(
                Arguments.of(WireFileAttribute.BIT_RATE, 0),
                Arguments.of(WireFileAttribute.LENGTH, 1),
                Arguments.of(WireFileAttribute.VARIABLE_BIT_RATE, 2),
                Arguments.of(WireFileAttribute.SAMPLE_RATE, 4),
                Arguments.of(WireFileAttribute.BIT_DEPTH, 5));
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
                Arguments.of(WireUserPresence.OFFLINE, 0),
                Arguments.of(WireUserPresence.AWAY, 1),
                Arguments.of(WireUserPresence.ONLINE, 2));
    }
}
