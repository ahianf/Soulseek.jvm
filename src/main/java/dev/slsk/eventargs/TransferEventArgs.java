// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import dev.slsk.Transfer;

/**
 * Base event arguments for transfer events.
 */
public class TransferEventArgs extends SoulseekClientEventArgs {
    private final Transfer transfer;

    TransferEventArgs(Transfer transfer) {
        this.transfer = transfer;
    }

    /**
     * Returns the transfer that raised the event.
     *
     * @return the transfer
     */
    public final Transfer getTransfer() {
        return transfer;
    }
}
