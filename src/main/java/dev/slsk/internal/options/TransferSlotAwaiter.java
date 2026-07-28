// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import dev.slsk.CancellationSignal;
import dev.slsk.internal.Transfer;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronously waits for an upload slot.
 */
@FunctionalInterface
public interface TransferSlotAwaiter {
    /**
     * Waits for a slot for the transfer.
     *
     * @param transfer the transfer
     * @param cancellationSignal the cancellation signal
     * @return a future completed when the slot is acquired
     */
    CompletableFuture<Void> awaitSlotAsync(Transfer transfer, CancellationSignal cancellationSignal);
}
