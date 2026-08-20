// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.soak;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.common.Monitors;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.network.tcp.SocketConnection;
import dev.slsk.internal.options.ConnectionOptions;
import java.io.OutputStream;
import java.util.Locale;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Scenario: a large loopback transfer.
 *
 * <p>Exercises the chunked read path at the 16 KiB default buffer, which is
 * where the per-chunk inactivity reschedule (defect 1.2) and the per-chunk
 * thread amplification (goal 2.1) do their damage. A 2 GiB transfer at that
 * buffer size is roughly 131,000 chunks.
 */
class LargeTransferSoak {

    private static final long TRANSFER_BYTES = 2L * 1024 * 1024 * 1024;

    @Test
    @DisplayName("Large transfer throughput, allocation and heap")
    void largeTransfer() throws Exception {
        try (LoopbackPeer peer =
                LoopbackPeer.start(LoopbackPeer.Behaviour.BYTE_SOURCE).withByteSourceLength(TRANSFER_BYTES)) {

            SocketConnection connection =
                    new SocketConnection(peer.endpoint(), new ConnectionOptions(), null, Monitors.shared());
            try {
                connection.connect(CancellationSignal.none());

                long heapBefore = HeapProbe.liveHeapBytes();
                long allocStart = HeapProbe.totalAllocatedBytes();
                long wallStart = System.nanoTime();

                CountingOutputStream sink = new CountingOutputStream();
                connection.read(
                        TRANSFER_BYTES,
                        java.nio.channels.Channels.newChannel(sink),
                        null,
                        null,
                        CancellationSignal.none());

                long wallElapsed = System.nanoTime() - wallStart;
                long allocated = HeapProbe.totalAllocatedBytes() - allocStart;
                int queueDepth = SchedulerProbe.connectionTimerQueueDepth();
                long heapAfter = HeapProbe.liveHeapBytes();

                double megabytesPerSecond = (TRANSFER_BYTES / (1024.0 * 1024.0)) / (wallElapsed / 1_000_000_000.0);
                long chunks = TRANSFER_BYTES / new ConnectionOptions().readBufferSize();

                SoakReport.record("large-transfer", "bytes", HeapProbe.formatBytes(TRANSFER_BYTES));
                SoakReport.record("large-transfer", "chunks (16 KiB buffer)", chunks);
                SoakReport.record(
                        "large-transfer", "throughput", String.format(Locale.ROOT, "%.0f MiB/s", megabytesPerSecond));
                SoakReport.record("large-transfer", "timer queue depth at end", queueDepth);
                SoakReport.record("large-transfer", "live heap before", HeapProbe.formatBytes(heapBefore));
                SoakReport.record("large-transfer", "live heap after", HeapProbe.formatBytes(heapAfter));
                if (allocStart >= 0) {
                    SoakReport.record("large-transfer", "allocated total", HeapProbe.formatBytes(allocated));
                    SoakReport.record(
                            "large-transfer", "allocated per chunk", HeapProbe.formatBytes(allocated / chunks));
                }

                assertEquals(TRANSFER_BYTES, sink.count(), "The transfer did not deliver every byte.");

                // The live set must not track transferred volume. This holds on
                // the baseline and must keep holding.
                assertTrue(
                        heapAfter < heapBefore + (256L * 1024 * 1024),
                        "Live heap grew by more than 256 MiB across a streamed transfer: before="
                                + HeapProbe.formatBytes(heapBefore) + " after=" + HeapProbe.formatBytes(heapAfter));
            } finally {
                connection.close();
            }
        }
    }

    /**
     * The timer queue must stay bounded across a large transfer.
     *
     * <p>On the 0.11.0 baseline a 2 GiB transfer ended with 131,077 entries,
     * one per 16 KiB chunk. Defect 1.2 removed the per-chunk reschedule.
     */
    @Test
    @DisplayName("Timer queue stays bounded across a large transfer")
    void timerQueueStaysBoundedAcrossTransfer() throws Exception {
        try (LoopbackPeer peer =
                LoopbackPeer.start(LoopbackPeer.Behaviour.BYTE_SOURCE).withByteSourceLength(TRANSFER_BYTES)) {

            int baseline = SchedulerProbe.connectionTimerQueueDepth();
            SocketConnection connection =
                    new SocketConnection(peer.endpoint(), new ConnectionOptions(), null, Monitors.shared());
            try {
                connection.connect(CancellationSignal.none());
                connection.read(
                        TRANSFER_BYTES,
                        java.nio.channels.Channels.newChannel(new CountingOutputStream()),
                        null,
                        null,
                        CancellationSignal.none());

                int depth = SchedulerProbe.connectionTimerQueueDepth();
                assertTrue(
                        depth <= baseline + 8,
                        "Timer queue grew across the transfer: baseline=" + baseline + " observed=" + depth);
            } finally {
                connection.close();
            }
        }
    }

    /** Discards bytes while counting them, so the sink is never the bottleneck. */
    private static final class CountingOutputStream extends OutputStream {
        private long count;

        long count() {
            return count;
        }

        @Override
        public void write(int singleByte) {
            count++;
        }

        @Override
        public void write(byte[] bytes, int offset, int length) {
            count += length;
        }
    }
}
