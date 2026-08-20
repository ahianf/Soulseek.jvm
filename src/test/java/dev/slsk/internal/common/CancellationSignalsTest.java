// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertSame;

import dev.slsk.internal.concurrent.CancellationController;
import dev.slsk.internal.concurrent.CancellationSignal;
import org.junit.jupiter.api.Test;

class CancellationSignalsTest {
    @Test
    void substitutesOnlyForNull() {
        try (CancellationController controller = new CancellationController()) {
            CancellationSignal signal = controller.getSignal();

            assertSame(signal, CancellationSignals.orNone(signal));
            assertSame(CancellationSignal.none(), CancellationSignals.orNone(null));
        }
    }
}
