// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import dev.slsk.Transfer;

/**
 * Event arguments raised by a transfer-progress update.
 */
public class TransferProgressUpdatedEventArgs extends TransferEventArgs {
    private final long previousBytesTransferred;

    /**
     * Creates transfer-progress event arguments.
     *
     * <p>The C# constructor is assembly-internal. This Java constructor is
     * public because package visibility cannot span the client and
     * event-argument packages.</p>
     */
    public TransferProgressUpdatedEventArgs(long previousBytesTransferred, Transfer transfer) {
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
