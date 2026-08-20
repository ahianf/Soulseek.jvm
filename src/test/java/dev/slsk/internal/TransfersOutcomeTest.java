// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.transfer.TransferDirection;
import dev.slsk.internal.transfer.TransferInternal;
import dev.slsk.internal.transfer.TransferTermination;
import dev.slsk.transfer.TransferOutcome;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** How a finished transfer's termination reason maps onto its public outcome. */
class TransfersOutcomeTest {

    /**
     * A size mismatch is ABORTED, and the peer's advertised size cannot
     * change between attempts — so retrying re-requests the same file to fail
     * the same way, peer-visibly, up to the attempt cap. The C# source
     * classifies the mismatch as terminal; the mapping used to fall through
     * to the generic retryable failure.
     */
    @Test
    @DisplayName("an aborted transfer maps to a failure that is not retried")
    void abortedIsNotRetryable() {
        TransferOutcome outcome = outcomeOf(TransferTermination.ABORTED);

        TransferOutcome.Failed failed = assertInstanceOf(TransferOutcome.Failed.class, outcome);
        assertFalse(failed.retryable(), "the peer's advertised size cannot change between attempts");
    }

    @Test
    @DisplayName("a plain error stays retryable")
    void erroredStaysRetryable() {
        TransferOutcome outcome = outcomeOf(TransferTermination.ERRORED);

        TransferOutcome.Failed failed = assertInstanceOf(TransferOutcome.Failed.class, outcome);
        assertTrue(failed.retryable());
    }

    @Test
    void cancelledMapsToCancelled() {
        assertEquals(new TransferOutcome.Cancelled(), outcomeOf(TransferTermination.CANCELLED));
    }

    private static TransferOutcome outcomeOf(TransferTermination termination) {
        TransferInternal transfer = new TransferInternal(TransferDirection.DOWNLOAD, "alice", "file", 42);
        transfer.complete(termination);
        return Transfers.outcomeOf(transfer.toTransfer());
    }
}
