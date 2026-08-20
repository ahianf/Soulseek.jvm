// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.Monitors;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.network.tcp.SocketConnection;
import dev.slsk.internal.options.ConnectionOptions;
import dev.slsk.internal.options.TransferOptions;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class TransferInternalTest {
    private static final Instant EPOCH = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void instantiatesWithGivenDataAndExpectedDefaults() {
        TransferOptions options = new TransferOptions();
        TransferInternal transfer = new TransferInternal(TransferDirection.DOWNLOAD, "alice", "file", 42, options);

        assertEquals(TransferDirection.DOWNLOAD, transfer.getDirection());
        assertEquals("alice", transfer.getUsername());
        assertEquals("file", transfer.getFilename());
        assertEquals(42, transfer.getToken());
        assertSame(options, transfer.getOptions());
        assertNull(transfer.getConnection());
        assertNull(transfer.getIpEndpoint());
        assertNull(transfer.getRemoteToken());
        assertNull(transfer.getSize());
        assertEquals(TransferPhase.NONE, transfer.getPhase());
        assertNull(transfer.getQueueLocation());
        assertNull(transfer.getTermination());
        assertEquals(0, transfer.getAverageSpeed());
        assertEquals(0, transfer.getBytesTransferred());
        assertEquals(0, transfer.getBytesRemaining());
        assertNull(transfer.getElapsedTime());
        assertNull(transfer.getRemainingTime());
        assertNull(transfer.getStartTime());
        assertNull(transfer.getEndTime());
        assertEquals(0, transfer.getPercentComplete());
        assertEquals(0, transfer.getStartOffset());
        assertNull(transfer.getException());
        assertNotNull(transfer.settlement());
        assertFalse(transfer.settlement().isSettled());
    }

    @Test
    void endpointDelegatesToConnection() {
        InetSocketAddress endpoint = new InetSocketAddress("127.0.0.1", 1234);
        SocketConnection connection = new SocketConnection(endpoint, new ConnectionOptions(), null, Monitors.shared());
        TransferInternal transfer = transfer(fixed(EPOCH), 1000);

        transfer.setConnection(connection);

        assertSame(connection, transfer.getConnection());
        assertEquals(endpoint, transfer.getIpEndpoint());
        connection.close();
    }

    @Test
    void waitKeyContainsExactSourceParts() {
        TransferInternal transfer = new TransferInternal(TransferDirection.UPLOAD, "alice", "file", -1);

        assertEquals(
                new WaitKey(Constants.WaitKey.TRANSFER, TransferDirection.UPLOAD, "alice", "file", -1),
                transfer.getWaitKey());
    }

    @Test
    void inProgressAndCompletedTransitionsSetTimesOnlyOnce() {
        MutableClock clock = new MutableClock(EPOCH);
        TransferInternal transfer = transfer(clock, 1000);

        transfer.setPhase(TransferPhase.IN_PROGRESS);
        assertEquals(EPOCH, transfer.getStartTime());
        assertNull(transfer.getEndTime());

        clock.advance(Duration.ofSeconds(1));
        transfer.setPhase(TransferPhase.IN_PROGRESS);
        assertEquals(EPOCH, transfer.getStartTime());

        transfer.complete(TransferTermination.SUCCEEDED);
        assertEquals(EPOCH.plusSeconds(1), transfer.getEndTime());
        assertEquals(Duration.ofSeconds(1), transfer.getElapsedTime());

        clock.advance(Duration.ofSeconds(1));
        transfer.complete(TransferTermination.SUCCEEDED);
        assertEquals(EPOCH.plusSeconds(1), transfer.getEndTime());
    }

    @Test
    void completedWithoutInProgressSetsEqualStartAndEnd() {
        TransferInternal transfer = transfer(fixed(EPOCH), 1000);

        transfer.complete(TransferTermination.SUCCEEDED);

        assertEquals(EPOCH, transfer.getStartTime());
        assertEquals(EPOCH, transfer.getEndTime());
        assertEquals(Duration.ZERO, transfer.getElapsedTime());
    }

    @Test
    void elapsedTimeUsesNowUntilCompletion() {
        MutableClock clock = new MutableClock(EPOCH);
        TransferInternal transfer = transfer(clock, 1000);
        assertNull(transfer.getElapsedTime());
        transfer.setPhase(TransferPhase.IN_PROGRESS);
        clock.advance(Duration.ofHours(24));
        assertEquals(Duration.ofHours(24), transfer.getElapsedTime());
    }

    @Test
    void startOffsetFastForwardsProgressAndComputations() {
        TransferInternal transfer = transfer(fixed(EPOCH), 1000);
        transfer.setSize(100L);
        transfer.setStartOffset(40);

        assertEquals(40, transfer.getStartOffset());
        assertEquals(40, transfer.getBytesTransferred());
        assertEquals(60, transfer.getBytesRemaining());
        assertEquals(40, transfer.getPercentComplete());
    }

    @Test
    void nullableSizeAndZeroSpeedProduceSourceDefaults() {
        TransferInternal transfer = transfer(fixed(EPOCH), 1000);
        transfer.updateProgress(12);
        assertEquals(-12, transfer.getBytesRemaining());
        assertEquals(0, transfer.getPercentComplete());
        assertNull(transfer.getRemainingTime());
    }

    @Test
    void progressBeforeStartChangesBytesButNotSpeed() {
        TransferInternal transfer = transfer(fixed(EPOCH), 0);

        transfer.updateProgress(100_000);

        assertEquals(100_000, transfer.getBytesTransferred());
        assertEquals(0, transfer.getAverageSpeed());
    }

    @Test
    void movingAverageInitializesAndSmoothsSubsequentUpdates() {
        MutableClock clock = new MutableClock(EPOCH);
        TransferInternal transfer = transfer(clock, 1000);
        transfer.setPhase(TransferPhase.IN_PROGRESS);
        clock.advance(Duration.ofSeconds(1));
        transfer.updateProgress(100);
        assertEquals(100, transfer.getAverageSpeed());

        clock.advance(Duration.ofSeconds(1));
        transfer.updateProgress(300);
        assertEquals(120, transfer.getAverageSpeed(), 0.000001);
    }

    @Test
    void reachingSizeComputesImmediateTotalSpeed() {
        MutableClock clock = new MutableClock(EPOCH);
        TransferInternal transfer = transfer(clock, 1000);
        transfer.setSize(100L);
        transfer.setPhase(TransferPhase.IN_PROGRESS);
        clock.advance(Duration.ofMillis(500));

        transfer.updateProgress(100);

        assertEquals(200, transfer.getAverageSpeed());
        assertEquals(Duration.ZERO, transfer.getRemainingTime());
    }

    @Test
    void completionAlwaysRecomputesFinalSpeedWithOneMillisecondFloor() {
        MutableClock clock = new MutableClock(EPOCH);
        TransferInternal transfer = transfer(clock, 1000);
        transfer.setStartOffset(10);
        transfer.setPhase(TransferPhase.IN_PROGRESS);
        transfer.updateProgress(20);

        transfer.complete(TransferTermination.SUCCEEDED);

        assertEquals(10_000, transfer.getAverageSpeed());
    }

    @Test
    void remainingTimeUsesTickPrecision() {
        MutableClock clock = new MutableClock(EPOCH);
        TransferInternal transfer = transfer(clock, 1000);
        transfer.setSize(3L);
        transfer.setPhase(TransferPhase.IN_PROGRESS);
        clock.advance(Duration.ofSeconds(1));
        transfer.updateProgress(1);

        assertEquals(Duration.ofSeconds(2), transfer.getRemainingTime());
    }

    @Test
    void mutablePropertiesRetainIdentityAndNullableValues() {
        TransferInternal transfer = transfer(fixed(EPOCH), 1000);
        RuntimeException failure = new RuntimeException("failure");
        transfer.setException(failure);
        transfer.setRemoteToken(-1);
        transfer.setSize(2L);

        assertSame(failure, transfer.getException());
        assertEquals(-1, transfer.getRemoteToken());
        assertEquals(2, transfer.getSize());
        transfer.setRemoteToken(null);
        transfer.setSize(null);
        assertNull(transfer.getRemoteToken());
        assertNull(transfer.getSize());
    }

    @Test
    void publicSnapshotCopiesCurrentState() {
        MutableClock clock = new MutableClock(EPOCH);
        TransferInternal internal = transfer(clock, 1000);
        internal.setSize(100L);
        internal.setStartOffset(10);
        internal.setRemoteToken(24);
        internal.setPhase(TransferPhase.IN_PROGRESS);
        clock.advance(Duration.ofSeconds(1));
        internal.updateProgress(60);
        RuntimeException failure = new RuntimeException("x");
        internal.setException(failure);

        Transfer transfer = internal.toTransfer();

        assertEquals(TransferDirection.DOWNLOAD, transfer.direction());
        assertEquals("alice", transfer.username());
        assertEquals("file", transfer.filename());
        assertEquals(42, transfer.token());
        assertEquals(TransferPhase.IN_PROGRESS, transfer.phase());
        assertNull(transfer.queueLocation());
        assertNull(transfer.termination());
        assertEquals(100, transfer.size());
        assertEquals(10, transfer.startOffset());
        assertEquals(60, transfer.bytesTransferred());
        assertEquals(50, transfer.averageSpeed());
        assertEquals(EPOCH, transfer.startTime());
        assertEquals(24, transfer.remoteToken());
        assertSame(failure, transfer.exception());
    }

    @Test
    void nullSizeSnapshotUsesZero() {
        Transfer transfer = transfer(fixed(EPOCH), 1000).toTransfer();
        assertEquals(0, transfer.size());
    }

    private static TransferInternal transfer(Clock clock, int progressUpdateLimit) {
        return new TransferInternal(TransferDirection.DOWNLOAD, "alice", "file", 42, null, clock, progressUpdateLimit);
    }

    private static Clock fixed(Instant instant) {
        return Clock.fixed(instant, ZoneOffset.UTC);
    }

    private static final class MutableClock extends Clock {
        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
