// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import dev.slsk.internal.transfer.Transfer;
import dev.slsk.internal.transfer.TransferPhase;
import java.util.Objects;

/**
 * A transfer state-change callback payload.
 *
 * @param previousState the transfer's previous state
 * @param transfer the transfer after the state change
 */
public record TransferStateChange(TransferPhase previousState, Transfer transfer) {
    /**
     * Creates a transfer state change.
     */
    public TransferStateChange {
        Objects.requireNonNull(previousState, "previousState");
    }
}
