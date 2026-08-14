// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.transfer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TransferModelTest {

    @Nested
    class ProgressTest {

        @Test
        void reportsFractionComplete() {
            assertEquals(0.5, Progress.of(50, 100, 10).fraction(), 1e-9);
            assertEquals(0d, Progress.of(0, 100, 0).fraction(), 1e-9);
            assertEquals(1d, Progress.of(100, 100, 10).fraction(), 1e-9);
        }

        @Test
        @DisplayName("an unknown size renders as zero rather than NaN")
        void unknownSizeGivesZeroFraction() {
            assertEquals(0d, Progress.of(500, 0, 10).fraction(), 1e-9);
        }

        @Test
        @DisplayName("over-delivery is clamped, since a progress bar cannot render 1.2")
        void fractionIsClamped() {
            assertEquals(1d, Progress.of(150, 100, 10).fraction(), 1e-9);
        }

        @Test
        void estimatesTimeRemainingFromTheRate() {
            assertEquals(
                    Optional.of(Duration.ofSeconds(5)), Progress.of(50, 100, 10).eta());
        }

        @Test
        @DisplayName("no estimate when there is no basis for one, rather than a wrong one")
        void withholdsTheEstimateWhenItCannotBeMade() {
            assertEquals(Optional.empty(), Progress.of(50, 100, 0).eta(), "no rate yet");
            assertEquals(Optional.empty(), Progress.of(50, 0, 10).eta(), "no size known");
            assertEquals(Optional.empty(), Progress.of(100, 100, 10).eta(), "already done");
            assertEquals(Optional.empty(), Progress.none(100).eta(), "not started");
        }

        @Test
        void reportsCompletion() {
            assertTrue(Progress.of(100, 100, 1).isComplete());
            assertFalse(Progress.of(99, 100, 1).isComplete());
            assertFalse(Progress.of(0, 0, 0).isComplete(), "unknown size is not complete");
        }

        @Test
        void rejectsNegativeMeasures() {
            assertThrows(IllegalArgumentException.class, () -> Progress.of(-1, 100, 1));
            assertThrows(IllegalArgumentException.class, () -> Progress.of(1, -1, 1));
            assertThrows(IllegalArgumentException.class, () -> Progress.of(1, 100, -1));
            assertThrows(NullPointerException.class, () -> new Progress(1, 100, 1, null));
        }
    }

    @Nested
    class StateTest {

        @Test
        void onlyFinishedIsTerminal() {
            assertTrue(new TransferState.Finished(new TransferOutcome.Cancelled()).isTerminal());
            assertFalse(new TransferState.Queued(0).isTerminal());
            assertFalse(new TransferState.Requesting().isTerminal());
            assertFalse(new TransferState.Connecting(false).isTerminal());
            assertFalse(new TransferState.Transferring(Progress.none(1)).isTerminal());
        }

        @Test
        @DisplayName("active means occupying a slot, which is what the concurrency caps count")
        void connectingAndTransferringAreActive() {
            assertTrue(new TransferState.Connecting(false).isActive());
            assertTrue(new TransferState.Transferring(Progress.none(1)).isActive());
            assertFalse(new TransferState.Queued(0).isActive());
            assertFalse(new TransferState.QueuedRemotely(OptionalInt.empty(), Instant.EPOCH).isActive());
            assertFalse(new TransferState.Finished(new TransferOutcome.Cancelled()).isActive());
        }

        @Test
        @DisplayName("a queue position a peer never reported is absent, not zero")
        void remoteQueuePositionIsOptional() {
            TransferState.QueuedRemotely unknown = new TransferState.QueuedRemotely(OptionalInt.empty(), Instant.EPOCH);
            assertTrue(unknown.position().isEmpty());
            assertEquals(
                    3,
                    new TransferState.QueuedRemotely(OptionalInt.of(3), Instant.EPOCH)
                            .position()
                            .getAsInt());
        }

        @Test
        void pausedCarriesWhereToResume() {
            TransferState.Paused paused = new TransferState.Paused(new TransferState.Queued(2));
            assertEquals(new TransferState.Queued(2), paused.resumeTo());
        }

        @Test
        @DisplayName("pausing a paused transfer is rejected, so resumeTo never nests")
        void pausedCannotNest() {
            TransferState.Paused paused = new TransferState.Paused(new TransferState.Requesting());
            assertThrows(IllegalArgumentException.class, () -> new TransferState.Paused(paused));
        }

        @Test
        void rejectsInvalidData() {
            assertThrows(IllegalArgumentException.class, () -> new TransferState.Queued(-1));
            assertThrows(NullPointerException.class, () -> new TransferState.Transferring(null));
            assertThrows(NullPointerException.class, () -> new TransferState.Finished(null));
        }

        @Test
        @DisplayName("a switch over the sealed hierarchy needs no default")
        void isExhaustivelySwitchable() {
            TransferState state = new TransferState.Transferring(Progress.of(1, 2, 3));
            String rendered =
                    switch (state) {
                        case TransferState.Queued queued -> "queued " + queued.localPosition();
                        case TransferState.Requesting ignored -> "requesting";
                        case TransferState.QueuedRemotely remote -> "remote " + remote.position();
                        case TransferState.Connecting connecting -> "connecting " + connecting.indirect();
                        case TransferState.Transferring transferring ->
                            "at " + transferring.progress().fraction();
                        case TransferState.Paused paused -> "paused";
                        case TransferState.Finished finished -> "done";
                    };
            assertEquals("at 0.5", rendered);
        }
    }

    @Nested
    class OutcomeTest {

        @Test
        void onlySucceededIsSuccess() {
            assertTrue(new TransferOutcome.Succeeded(1, Duration.ZERO).isSuccess());
            assertFalse(new TransferOutcome.Cancelled().isSuccess());
            assertFalse(new TransferOutcome.Rejected(RejectionReason.QUEUE_FULL, "full").isSuccess());
            assertFalse(new TransferOutcome.Failed(new RuntimeException(), true).isSuccess());
        }

        @Test
        @DisplayName("the peer's own words are kept, so an unknown reason is still renderable")
        void rejectionKeepsTheRawMessage() {
            TransferOutcome.Rejected rejected = new TransferOutcome.Rejected(RejectionReason.UNKNOWN, "Something odd");
            assertEquals("Something odd", rejected.rawMessage());
            assertEquals(RejectionReason.UNKNOWN, rejected.reason());
        }

        @Test
        void rejectsInvalidData() {
            assertThrows(NullPointerException.class, () -> new TransferOutcome.Rejected(null, "m"));
            assertThrows(NullPointerException.class, () -> new TransferOutcome.Rejected(RejectionReason.BANNED, null));
            assertThrows(NullPointerException.class, () -> new TransferOutcome.Failed(null, false));
            assertThrows(IllegalArgumentException.class, () -> new TransferOutcome.Succeeded(-1, Duration.ZERO));
        }

        @Test
        @DisplayName("a switch over the sealed hierarchy needs no default")
        void isExhaustivelySwitchable() {
            TransferOutcome outcome = new TransferOutcome.Rejected(RejectionReason.BANNED, "banned");
            String rendered =
                    switch (outcome) {
                        case TransferOutcome.Succeeded succeeded -> "ok";
                        case TransferOutcome.Cancelled ignored -> "cancelled";
                        case TransferOutcome.Rejected rejected -> "refused: " + rejected.reason();
                        case TransferOutcome.Failed failed -> "failed";
                    };
            assertEquals("refused: BANNED", rendered);
        }
    }
}
