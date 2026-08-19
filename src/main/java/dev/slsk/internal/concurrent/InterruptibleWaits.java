// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.concurrent;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.function.BooleanSupplier;

/** Shared race-aware blocking waits used by the public facets. */
public final class InterruptibleWaits {

    private InterruptibleWaits() {}

    /** Waits for a latch while preserving an interrupt that follows completion. */
    public static void await(CountDownLatch latch, BooleanSupplier completed) throws InterruptedException {
        Objects.requireNonNull(latch, "latch");
        Objects.requireNonNull(completed, "completed");
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            if (completed.getAsBoolean()) {
                Thread.currentThread().interrupt();
                return;
            }
            throw interrupted;
        }
    }
}
