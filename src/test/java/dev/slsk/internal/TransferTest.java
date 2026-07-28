// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

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
                TransferState.IN_PROGRESS,
                1000,
                10,
                400,
                200,
                startTime,
                endTime,
                24,
                endpoint,
                exception);

        assertEquals(TransferDirection.DOWNLOAD, transfer.getDirection());
        assertEquals("alice", transfer.getUsername());
        assertEquals("music/file.mp3", transfer.getFilename());
        assertEquals(42, transfer.getToken());
        assertEquals(TransferState.IN_PROGRESS, transfer.getState());
        assertEquals(1000, transfer.getSize());
        assertEquals(10, transfer.getStartOffset());
        assertEquals(400, transfer.getBytesTransferred());
        assertEquals(200, transfer.getAverageSpeed());
        assertEquals(startTime, transfer.getStartTime());
        assertEquals(endTime, transfer.getEndTime());
        assertEquals(24, transfer.getRemoteToken());
        assertSame(endpoint, transfer.getIpEndpoint());
        assertSame(exception, transfer.getException());
        assertEquals(600, transfer.getBytesRemaining());
        assertEquals(Duration.ofDays(1), transfer.getElapsedTime());
        assertEquals(40, transfer.getPercentComplete());
        assertEquals(Duration.ofSeconds(3), transfer.getRemainingTime());
    }

    @Test
    @DisplayName("Optional values use source defaults")
    void optionalValuesUseSourceDefaults() {
        Transfer transfer = new Transfer(TransferDirection.UPLOAD, null, null, 0, TransferState.NONE, 0, 0);

        assertNull(transfer.getUsername());
        assertNull(transfer.getFilename());
        assertEquals(0, transfer.getBytesTransferred());
        assertEquals(0, transfer.getAverageSpeed());
        assertNull(transfer.getStartTime());
        assertNull(transfer.getEndTime());
        assertNull(transfer.getRemoteToken());
        assertNull(transfer.getIpEndpoint());
        assertNull(transfer.getException());
        assertEquals(0, transfer.getPercentComplete());
        assertNull(transfer.getElapsedTime());
        assertNull(transfer.getRemainingTime());
    }

    @Test
    @DisplayName("Every optional-parameter overload delegates to the canonical constructor")
    void optionalParameterOverloadsDelegateToCanonicalConstructor() {
        Instant start = Instant.parse("2020-01-01T00:00:00Z");
        Instant end = start.plusSeconds(1);
        InetSocketAddress endpoint = new InetSocketAddress("127.0.0.1", 1);

        assertEquals(
                1,
                new Transfer(TransferDirection.DOWNLOAD, "u", "f", 1, TransferState.NONE, 2, 0, 1)
                        .getBytesTransferred());
        assertEquals(
                2,
                new Transfer(TransferDirection.DOWNLOAD, "u", "f", 1, TransferState.NONE, 3, 0, 1, 2)
                        .getAverageSpeed());
        assertEquals(
                start,
                new Transfer(TransferDirection.DOWNLOAD, "u", "f", 1, TransferState.NONE, 3, 0, 1, 2, start)
                        .getStartTime());
        assertEquals(
                end,
                new Transfer(TransferDirection.DOWNLOAD, "u", "f", 1, TransferState.NONE, 3, 0, 1, 2, start, end)
                        .getEndTime());
        assertEquals(
                5,
                new Transfer(TransferDirection.DOWNLOAD, "u", "f", 1, TransferState.NONE, 3, 0, 1, 2, start, end, 5)
                        .getRemoteToken());
        assertSame(
                endpoint,
                new Transfer(
                                TransferDirection.DOWNLOAD,
                                "u",
                                "f",
                                1,
                                TransferState.NONE,
                                3,
                                0,
                                1,
                                2,
                                start,
                                end,
                                5,
                                endpoint)
                        .getIpEndpoint());
    }

    @Test
    @DisplayName("ElapsedTime uses the current time when EndTime is null")
    void elapsedTimeUsesCurrentTimeWhenEndTimeIsNull() {
        Instant before = Instant.now().minusSeconds(2);
        Transfer transfer =
                new Transfer(TransferDirection.DOWNLOAD, "u", "f", 1, TransferState.IN_PROGRESS, 1, 0, 0, 1, before);

        assertTrue(transfer.getElapsedTime().compareTo(Duration.ofSeconds(2)) >= 0);
        assertTrue(transfer.getElapsedTime().compareTo(Duration.ofSeconds(5)) < 0);
    }

    @Test
    @DisplayName("PercentComplete returns zero if Size is zero")
    void percentCompleteReturnsZeroIfSizeIsZero() {
        Transfer transfer = new Transfer(TransferDirection.DOWNLOAD, "u", "f", 1, TransferState.NONE, 0, 0, 10);

        assertEquals(0, transfer.getPercentComplete());
        assertEquals(-10, transfer.getBytesRemaining());
    }

    @Test
    @DisplayName("RemainingTime truncates to source TimeSpan 100-nanosecond ticks")
    void remainingTimeTruncatesToSourceTimeSpanTicks() {
        Transfer positive = new Transfer(TransferDirection.DOWNLOAD, "u", "f", 1, TransferState.NONE, 1, 0, 0, 600);
        Transfer negative = new Transfer(TransferDirection.DOWNLOAD, "u", "f", 1, TransferState.NONE, -1, 0, 0, 600);

        assertEquals(Duration.ofNanos(1_666_600), positive.getRemainingTime());
        assertEquals(Duration.ofNanos(-1_666_600), negative.getRemainingTime());
    }

    @Test
    @DisplayName("Rejects null value-type mappings")
    void rejectsNullValueTypeMappings() {
        assertThrows(NullPointerException.class, () -> new Transfer(null, "u", "f", 1, TransferState.NONE, 1, 0));
        assertThrows(
                NullPointerException.class, () -> new Transfer(TransferDirection.DOWNLOAD, "u", "f", 1, null, 1, 0));
    }

    @Test
    @DisplayName("Rejects a non-finite remaining duration")
    void rejectsNonFiniteRemainingDuration() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new Transfer(TransferDirection.DOWNLOAD, "u", "f", 1, TransferState.NONE, 1, 0, 0, Double.NaN));
        assertThrows(
                ArithmeticException.class,
                () -> new Transfer(
                        TransferDirection.DOWNLOAD, "u", "f", 1, TransferState.NONE, 1, 0, 0, Double.MIN_VALUE));
    }
}
