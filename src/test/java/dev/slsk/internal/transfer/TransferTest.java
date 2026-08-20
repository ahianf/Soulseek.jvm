// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * One constructor now. The seven shorter ones were default-parameter expansion
 * with no production caller — this test class was their only user —
 * and they went the way of the transfer overloads before them.
 */
class TransferTest {
    @Test
    @DisplayName("Instantiates with expected data")
    void instantiatesWithExpectedData() {
        Instant startTime = Instant.parse("2019-04-25T00:00:00Z");
        Instant endTime = Instant.parse("2019-04-26T00:00:00Z");
        InetSocketAddress endpoint = new InetSocketAddress("127.0.0.1", 1234);
        RuntimeException exception = new RuntimeException("failure");

        Transfer transfer = new Transfer(
                TransferDirection.DOWNLOAD,
                "alice",
                "music/file.mp3",
                42,
                TransferPhase.IN_PROGRESS,
                null,
                null,
                1000,
                10,
                400,
                200,
                startTime,
                endTime,
                24,
                endpoint,
                exception);

        assertEquals(TransferDirection.DOWNLOAD, transfer.direction());
        assertEquals("alice", transfer.username());
        assertEquals("music/file.mp3", transfer.filename());
        assertEquals(42, transfer.token());
        assertEquals(TransferPhase.IN_PROGRESS, transfer.phase());
        assertNull(transfer.queueLocation());
        assertNull(transfer.termination());
        assertEquals(1000, transfer.size());
        assertEquals(10, transfer.startOffset());
        assertEquals(400, transfer.bytesTransferred());
        assertEquals(200, transfer.averageSpeed());
        assertEquals(startTime, transfer.startTime());
        assertEquals(endTime, transfer.endTime());
        assertEquals(24, transfer.remoteToken());
        assertSame(endpoint, transfer.ipEndpoint());
        assertSame(exception, transfer.exception());
        assertEquals(600, transfer.bytesRemaining());
        assertEquals(Duration.ofDays(1), transfer.elapsedTime());
        assertEquals(40, transfer.percentComplete());
        assertEquals(Duration.ofSeconds(3), transfer.remainingTime());
    }

    @Test
    @DisplayName("Optional values use source defaults")
    void optionalValuesUseSourceDefaults() {
        Transfer transfer = transfer(TransferPhase.NONE, 0, 0, 0);

        assertEquals(0, transfer.bytesTransferred());
        assertEquals(0, transfer.averageSpeed());
        assertNull(transfer.startTime());
        assertNull(transfer.endTime());
        assertNull(transfer.remoteToken());
        assertNull(transfer.ipEndpoint());
        assertNull(transfer.exception());
        assertEquals(0, transfer.percentComplete());
        assertNull(transfer.elapsedTime());
        assertNull(transfer.remainingTime());
    }

    @Test
    @DisplayName("ElapsedTime uses the current time when EndTime is null")
    void elapsedTimeUsesCurrentTimeWhenEndTimeIsNull() {
        Instant before = Instant.now().minusSeconds(2);
        Transfer transfer = new Transfer(
                TransferDirection.DOWNLOAD,
                "u",
                "f",
                1,
                TransferPhase.IN_PROGRESS,
                null,
                null,
                1,
                0,
                0,
                1,
                before,
                null,
                null,
                null,
                null);

        assertTrue(transfer.elapsedTime().compareTo(Duration.ofSeconds(2)) >= 0);
        assertTrue(transfer.elapsedTime().compareTo(Duration.ofSeconds(5)) < 0);
    }

    @Test
    @DisplayName("PercentComplete returns zero if Size is zero")
    void percentCompleteReturnsZeroIfSizeIsZero() {
        Transfer transfer = transfer(TransferPhase.NONE, 0, 10, 0);

        assertEquals(0, transfer.percentComplete());
        assertEquals(-10, transfer.bytesRemaining());
    }

    @Test
    @DisplayName("RemainingTime retains nanosecond precision")
    void remainingTimeRetainsNanosecondPrecision() {
        Transfer positive = transfer(TransferPhase.NONE, 1, 0, 600);
        Transfer negative = transfer(TransferPhase.NONE, -1, 0, 600);

        assertEquals(Duration.ofNanos(1_666_666), positive.remainingTime());
        assertEquals(Duration.ofNanos(-1_666_666), negative.remainingTime());
    }

    @Test
    @DisplayName("Rejects null value-type mappings")
    void rejectsNullValueTypeMappings() {
        assertThrows(
                NullPointerException.class,
                () -> new Transfer(
                        null, "u", "f", 1, TransferPhase.NONE, null, null, 1, 0, 0, 0, null, null, null, null, null));
        assertThrows(
                NullPointerException.class,
                () -> new Transfer(
                        TransferDirection.DOWNLOAD,
                        "u",
                        "f",
                        1,
                        null,
                        null,
                        null,
                        1,
                        0,
                        0,
                        0,
                        null,
                        null,
                        null,
                        null,
                        null));
    }

    @Test
    @DisplayName("Rejects a non-finite remaining duration")
    void rejectsNonFiniteRemainingDuration() {
        assertThrows(IllegalArgumentException.class, () -> transfer(TransferPhase.NONE, 1, 0, Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> transfer(TransferPhase.NONE, 1, 0, Double.MIN_VALUE));
    }

    @Test
    void rejectsInconsistentPhaseDetails() {
        assertThrows(IllegalArgumentException.class, () -> transfer(TransferPhase.QUEUED, null, null));
        assertThrows(
                IllegalArgumentException.class, () -> transfer(TransferPhase.NONE, TransferQueueLocation.LOCAL, null));
        assertThrows(IllegalArgumentException.class, () -> transfer(TransferPhase.COMPLETED, null, null));
        assertThrows(
                IllegalArgumentException.class,
                () -> transfer(TransferPhase.IN_PROGRESS, null, TransferTermination.SUCCEEDED));
    }

    private static Transfer transfer(TransferPhase phase, long size, long bytesTransferred, double averageSpeed) {
        return new Transfer(
                TransferDirection.DOWNLOAD,
                "u",
                "f",
                1,
                phase,
                null,
                null,
                size,
                0,
                bytesTransferred,
                averageSpeed,
                null,
                null,
                null,
                null,
                null);
    }

    private static Transfer transfer(
            TransferPhase phase, TransferQueueLocation queueLocation, TransferTermination termination) {
        return new Transfer(
                TransferDirection.DOWNLOAD,
                "u",
                "f",
                1,
                phase,
                queueLocation,
                termination,
                1,
                0,
                0,
                0,
                null,
                null,
                null,
                null,
                null);
    }
}
