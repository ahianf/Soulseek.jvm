// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ServerSessionInfoTest {
    @Test
    @DisplayName("Instantiates with given values")
    void instantiatesWithGivenValues() {
        ServerSessionInfo info = new ServerSessionInfo(1, 2, 3, false);

        assertEquals(1, info.parentMinSpeed());
        assertEquals(2, info.parentSpeedRatio());
        assertEquals(3, info.wishlistInterval());
        assertFalse(info.supporter());
    }

    @Test
    @DisplayName("Optional constructor values default to null")
    void optionalConstructorValuesDefaultToNull() {
        ServerSessionInfo empty = new ServerSessionInfo();
        ServerSessionInfo one = new ServerSessionInfo(1);
        ServerSessionInfo two = new ServerSessionInfo(1, 2);
        ServerSessionInfo three = new ServerSessionInfo(1, 2, 3);

        assertNull(empty.parentMinSpeed());
        assertNull(empty.parentSpeedRatio());
        assertNull(empty.wishlistInterval());
        assertNull(empty.supporter());
        assertNull(one.parentSpeedRatio());
        assertNull(two.wishlistInterval());
        assertNull(three.supporter());
    }

    @Test
    @DisplayName("With overlays substitutions")
    void withOverlaysSubstitutions() {
        ServerSessionInfo info = new ServerSessionInfo().with(1, 2, 3, false);

        assertEquals(1, info.parentMinSpeed());
        assertEquals(2, info.parentSpeedRatio());
        assertEquals(3, info.wishlistInterval());
        assertFalse(info.supporter());
    }

    @Test
    @DisplayName("With does not overlay nulls")
    void withDoesNotOverlayNulls() {
        ServerSessionInfo original = new ServerSessionInfo(1, 2, 3, false);

        ServerSessionInfo info = original.with(null, null, null, null);

        assertEquals(1, info.parentMinSpeed());
        assertEquals(2, info.parentSpeedRatio());
        assertEquals(3, info.wishlistInterval());
        assertFalse(info.supporter());
    }

    @Test
    @DisplayName("With can overlay a subset")
    void withCanOverlaySubset() {
        ServerSessionInfo info = new ServerSessionInfo(1, 2, 3, false).with(null, 20, null, true);

        assertEquals(1, info.parentMinSpeed());
        assertEquals(20, info.parentSpeedRatio());
        assertEquals(3, info.wishlistInterval());
        assertEquals(true, info.supporter());
    }
}
