// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import dev.slsk.internal.transfer.Transfer;

/**
 * Receives upload slot releases.
 */
@FunctionalInterface
public interface TransferSlotReleasedCallback {
    /**
     * Handles release of a transfer's slot.
     *
     * @param transfer the transfer
     */
    void onSlotReleased(Transfer transfer);
}
