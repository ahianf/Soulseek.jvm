// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ConstantsTest {
    @Test
    void preservesEverySourceConstant() {
        assertEquals(170, Constants.MAJOR_VERSION);
        assertEquals("Direct", Constants.ConnectionMethod.DIRECT);
        assertEquals("Indirect", Constants.ConnectionMethod.INDIRECT);
        assertEquals("D", Constants.ConnectionType.DISTRIBUTED);
        assertEquals("P", Constants.ConnectionType.PEER);
        assertEquals("F", Constants.ConnectionType.TRANSFER);
    }
}
