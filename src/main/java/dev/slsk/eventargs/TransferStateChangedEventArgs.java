// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import dev.slsk.Transfer;
import dev.slsk.TransferStates;
import java.util.Objects;

/**
 * Event arguments raised by a transfer-state change.
 */
public class TransferStateChangedEventArgs extends TransferEventArgs {
    private final TransferStates previousState;

    TransferStateChangedEventArgs(TransferStates previousState, Transfer transfer) {
        super(transfer);
        this.previousState = Objects.requireNonNull(previousState, "previousState");
    }

    /**
     * Returns the previous transfer state.
     *
     * @return the previous state
     */
    public final TransferStates getPreviousState() {
        return previousState;
    }
}
