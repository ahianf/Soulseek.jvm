// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class WaitKeyTest {
    @Test
    @DisplayName("Instantiates with parts")
    void instantiatesWithParts() {
        WaitKey key = new WaitKey(1, 2);

        assertNotNull(key);
        assertArrayEquals(new Object[] {1, 2}, key.getTokenParts());
    }

    @Test
    @DisplayName("Instantiates with no parts")
    void instantiatesWithNoParts() {
        WaitKey key = new WaitKey();

        assertEquals("", key.getToken());
        assertEquals(0, key.getTokenParts().length);
        assertEquals(0, key.hashCode());
    }

    @Test
    @DisplayName("Token joins all parts")
    void tokenJoinsAllParts() {
        WaitKey key = new WaitKey(1, "test", new BigDecimal("5"), null);

        assertEquals("1:test:5:", key.getToken());
        assertEquals(key.getToken(), key.toString());
    }

    @Test
    @DisplayName("Equal keys have equal hashes")
    void equalKeysHaveEqualHashes() {
        WaitKey first = new WaitKey(1, "test", new BigDecimal("5"));
        WaitKey second = new WaitKey(1, "test", new BigDecimal("5"));

        assertEquals(first, second);
        assertEquals(second, first);
        assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    @DisplayName("Different tokens are not equal")
    void differentTokensAreNotEqual() {
        WaitKey first = new WaitKey(1, "test");
        WaitKey second = new WaitKey(2, "test");

        assertNotEquals(first, second);
        assertFalse(first.equals("1:test"));
    }

    @Test
    @DisplayName("Equality is based on joined token rather than part boundaries")
    void equalityUsesJoinedToken() {
        WaitKey first = new WaitKey("a:b", "c");
        WaitKey second = new WaitKey("a", "b:c");

        assertEquals(first, second);
        assertTrue(first.equals(second));
    }

    @Test
    @DisplayName("Token parts preserve the original array alias")
    void tokenPartsPreserveOriginalArrayAlias() {
        Object[] parts = {1, 2};
        WaitKey key = new WaitKey(parts);

        assertSame(parts, key.getTokenParts());
        parts[0] = 3;

        assertArrayEquals(new Object[] {3, 2}, key.getTokenParts());
        assertEquals("1:2", key.getToken());
    }

    @Test
    @DisplayName("Null equality preserves the source exception")
    void nullEqualityPreservesSourceException() {
        WaitKey key = new WaitKey("test");

        assertThrows(NullPointerException.class, () -> key.equals(null));
    }

    @Test
    @DisplayName("Explicit null parts array is rejected")
    void explicitNullPartsArrayIsRejected() {
        assertThrows(NullPointerException.class, () -> new WaitKey((Object[]) null));
    }
}
