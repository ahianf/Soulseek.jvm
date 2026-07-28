// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.Transfer;

/**
 * Base event payload for transfer events.
 */
public class TransferEvent extends SoulseekClientEvent {
    private final Transfer transfer;

    TransferEvent(Transfer transfer) {
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
