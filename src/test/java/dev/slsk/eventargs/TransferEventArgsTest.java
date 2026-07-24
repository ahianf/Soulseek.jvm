// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.Transfer;
import dev.slsk.TransferDirection;
import dev.slsk.TransferStates;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TransferEventArgsTest {
    @Test
    @DisplayName("TransferEventArgs instantiates with the given data")
    void transferEventArgsInstantiatesWithTheGivenData() {
        Transfer transfer = transfer();
        TransferEventArgs args = new TransferEventArgs(transfer);

        assertSame(transfer, args.getTransfer());
    }

    @Test
    @DisplayName("TransferProgressUpdatedEventArgs instantiates with the given data")
    void progressUpdatedInstantiatesWithTheGivenData() {
        Transfer transfer = transfer();
        TransferProgressUpdatedEventArgs args = new TransferProgressUpdatedEventArgs(-1, transfer);

        assertEquals(-1, args.getPreviousBytesTransferred());
        assertSame(transfer, args.getTransfer());
    }

    @Test
    @DisplayName("TransferStateChangedEventArgs instantiates with the given data")
    void stateChangedInstantiatesWithTheGivenData() {
        Transfer transfer = transfer();
        TransferStates previousState = TransferStates.IN_PROGRESS.or(TransferStates.ERRORED);
        TransferStateChangedEventArgs args = new TransferStateChangedEventArgs(previousState, transfer);

        assertEquals(previousState, args.getPreviousState());
        assertSame(transfer, args.getTransfer());
    }

    @Test
    @DisplayName("Base event arguments preserve a null transfer")
    void baseEventArgsPreserveNullTransfer() {
        assertNull(new TransferEventArgs(null).getTransfer());
        assertNull(new TransferProgressUpdatedEventArgs(0, null).getTransfer());
    }

    @Test
    @DisplayName("Rejects null previous state because C# flags are non-nullable")
    void rejectsNullPreviousState() {
        assertThrows(NullPointerException.class, () -> new TransferStateChangedEventArgs(null, transfer()));
    }

    private static Transfer transfer() {
        return new Transfer(TransferDirection.DOWNLOAD, "alice", "file.mp3", 42, TransferStates.IN_PROGRESS, 100, 0);
    }
}
