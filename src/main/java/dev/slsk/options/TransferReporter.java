// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import dev.slsk.Transfer;

/**
 * Reports per-chunk transfer statistics.
 */
@FunctionalInterface
public interface TransferReporter {
    /**
     * Reports a transfer chunk.
     *
     * @param transfer the transfer
     * @param attemptedBytes the attempted byte count
     * @param grantedBytes the governor-granted byte count
     * @param transferredBytes the transferred byte count
     */
    void report(Transfer transfer, int attemptedBytes, int grantedBytes, int transferredBytes);
}
