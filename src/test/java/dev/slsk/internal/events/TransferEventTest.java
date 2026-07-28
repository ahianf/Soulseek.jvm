// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.internal.Transfer;
import dev.slsk.internal.TransferDirection;
import dev.slsk.internal.TransferState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TransferEventTest {
    @Test
    @DisplayName("TransferEvent instantiates with the given data")
    void transferEventInstantiatesWithTheGivenData() {
        Transfer transfer = transfer();
        TransferEvent args = new TransferEvent(transfer);

        assertSame(transfer, args.getTransfer());
    }

    @Test
    @DisplayName("TransferProgressUpdatedEvent instantiates with the given data")
    void progressUpdatedInstantiatesWithTheGivenData() {
        Transfer transfer = transfer();
        TransferProgressUpdatedEvent args = new TransferProgressUpdatedEvent(-1, transfer);

        assertEquals(-1, args.getPreviousBytesTransferred());
        assertSame(transfer, args.getTransfer());
    }

    @Test
    @DisplayName("TransferStateChangedEvent instantiates with the given data")
    void stateChangedInstantiatesWithTheGivenData() {
        Transfer transfer = transfer();
        TransferState previousState = TransferState.IN_PROGRESS.or(TransferState.ERRORED);
        TransferStateChangedEvent args = new TransferStateChangedEvent(previousState, transfer);

        assertEquals(previousState, args.getPreviousState());
        assertSame(transfer, args.getTransfer());
    }

    @Test
    @DisplayName("Base event arguments preserve a null transfer")
    void baseEventPreserveNullTransfer() {
        assertNull(new TransferEvent(null).getTransfer());
        assertNull(new TransferProgressUpdatedEvent(0, null).getTransfer());
    }

    @Test
    @DisplayName("Rejects null previous state because C# flags are non-nullable")
    void rejectsNullPreviousState() {
        assertThrows(NullPointerException.class, () -> new TransferStateChangedEvent(null, transfer()));
    }

    private static Transfer transfer() {
        return new Transfer(TransferDirection.DOWNLOAD, "alice", "file.mp3", 42, TransferState.IN_PROGRESS, 100, 0);
    }
}
