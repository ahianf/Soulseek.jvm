// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import dev.slsk.Transfer;
import dev.slsk.TransferState;
import java.util.Objects;

/**
 * Event arguments raised by a transfer-state change.
 */
public class TransferStateChangedEvent extends TransferEvent {
    private final TransferState previousState;

    /**
     * Creates transfer-state event payload.
     *
     * <p>The C# constructor is assembly-internal. This Java constructor is
     * public because package visibility cannot span the client and
     * event-argument packages.</p>
     */
    public TransferStateChangedEvent(TransferState previousState, Transfer transfer) {
        super(transfer);
        this.previousState = Objects.requireNonNull(previousState, "previousState");
    }

    /**
     * Returns the previous transfer state.
     *
     * @return the previous state
     */
    public final TransferState getPreviousState() {
        return previousState;
    }
}
