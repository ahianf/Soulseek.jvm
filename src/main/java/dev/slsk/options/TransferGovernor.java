// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import dev.slsk.CancellationToken;
import dev.slsk.Transfer;
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
     * @param cancellationToken the cancellation token
     * @return a future containing the granted byte count
     */
    CompletableFuture<Integer> grantAsync(Transfer transfer, int requestedBytes, CancellationToken cancellationToken);
}
