// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.Transfer;

/**
 * Event arguments raised by a transfer-progress update.
 */
public class TransferProgressUpdatedEvent extends TransferEvent {
    private final long previousBytesTransferred;

    /**
     * Creates transfer-progress event payload.
     *
     * <p>The C# constructor is assembly-internal. This Java constructor is
     * public because package visibility cannot span the client and
     * event-argument packages.</p>
     */
    public TransferProgressUpdatedEvent(long previousBytesTransferred, Transfer transfer) {
        super(transfer);
        this.previousBytesTransferred = previousBytesTransferred;
    }

    /**
     * Returns the byte count immediately before the event.
     *
     * @return the previous byte count
     */
    public final long getPreviousBytesTransferred() {
        return previousBytesTransferred;
    }
}
