// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.internal.concurrent.CancellationSignal;

/** Internal cancellation-signal defaults. */
public final class CancellationSignals {
    private CancellationSignals() {}

    /** Returns the supplied signal, or the uncancellable singleton for {@code null}. */
    public static CancellationSignal orNone(CancellationSignal signal) {
        return signal == null ? CancellationSignal.none() : signal;
    }
}
