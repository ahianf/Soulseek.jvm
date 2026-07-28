// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import dev.slsk.CancellationSignal;
import dev.slsk.internal.Transfer;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronously grants bytes to govern transfer speed.
 */
@FunctionalInterface
public interface TransferGovernor {
    /**
     * Grants some or all of the requested bytes.
     *
     * @param transfer the transfer
     * @param requestedBytes the requested byte count
     * @param cancellationSignal the cancellation signal
     * @return a future containing the granted byte count
     */
    CompletableFuture<Integer> grantAsync(Transfer transfer, int requestedBytes, CancellationSignal cancellationSignal);
}
