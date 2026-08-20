// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.soak;

import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.common.Monitors;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.network.DefaultMessageConnection;
import dev.slsk.internal.options.ConnectionOptions;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Scenario: sustained framed-message flood on a single connection.
 *
 * <p>This is the distributed-search hot path. Every frame currently costs
 * roughly six virtual-thread creations, two {@code CopyOnWriteArrayList}
 * copies, and several reschedules of the inactivity timeout. The measurements
 * here are what Phase 3 has to improve.
 */
class MessageFloodSoak {

    private static final long RUN_MILLIS = 5_000;

    @Test
    @DisplayName("Framed message throughput and cost")
    void messageThroughput() throws Exception {
        try (LoopbackPeer peer =
                LoopbackPeer.start(LoopbackPeer.Behaviour.FRAME_FLOOD).withFramePayloadSize(32)) {

            AtomicLong messages = new AtomicLong();
            ConnectionOptions options = new ConnectionOptions();

            DefaultMessageConnection connection =
                    new DefaultMessageConnection(peer.endpoint(), options, 4, null, Monitors.shared());
            connection.addMessageReadListener(event -> messages.incrementAndGet());

            long allocStart = HeapProbe.totalAllocatedBytes();
            long cpuStart = CpuProbe.processCpuNanos();
            long wallStart = System.nanoTime();

            connection.connect(CancellationSignal.none());
            Thread.sleep(RUN_MILLIS);

            long wallElapsed = System.nanoTime() - wallStart;
            long count = messages.get();
            long allocated = HeapProbe.totalAllocatedBytes() - allocStart;
            long cpu = CpuProbe.processCpuNanos() - cpuStart;
            int queueDepth = SchedulerProbe.connectionTimerQueueDepth();

            double perSecond = count / (wallElapsed / 1_000_000_000.0);
            SoakReport.record("message-flood", "messages read", count);
            SoakReport.record("message-flood", "messages/sec", String.format(Locale.ROOT, "%.0f", perSecond));
            SoakReport.record("message-flood", "timer queue depth during flood", queueDepth);
            if (allocStart >= 0) {
                SoakReport.record("message-flood", "allocated total", HeapProbe.formatBytes(allocated));
                SoakReport.record(
                        "message-flood",
                        "allocated per message",
                        count == 0 ? "n/a" : HeapProbe.formatBytes(allocated / count));
            }
            if (cpuStart >= 0) {
                SoakReport.record(
                        "message-flood",
                        "cpu cores used",
                        String.format(Locale.ROOT, "%.2f", (double) cpu / wallElapsed));
            }

            connection.close();

            assertTrue(count > 0, "The flood peer produced no readable frames; the harness is broken.");
        }
    }

    /**
     * The timer queue must not grow while a connection reads.
     *
     * <p>On the 0.11.0 baseline this reached 835,720 entries: every framed read
     * rescheduled the inactivity timeout and the executor never evicted the
     * cancelled tasks. Defect 1.2 replaced the per-chunk reschedule with a
     * periodic monitor that reads {@code lastActivityNanos}.
     */
    @Test
    @DisplayName("Timer queue stays bounded under message flood")
    void timerQueueStaysBoundedUnderFlood() throws Exception {
        try (LoopbackPeer peer =
                LoopbackPeer.start(LoopbackPeer.Behaviour.FRAME_FLOOD).withFramePayloadSize(32)) {

            int baseline = SchedulerProbe.connectionTimerQueueDepth();
            DefaultMessageConnection connection =
                    new DefaultMessageConnection(peer.endpoint(), new ConnectionOptions(), 4, null, Monitors.shared());
            try {
                connection.connect(CancellationSignal.none());
                Thread.sleep(TimeUnit.SECONDS.toMillis(3));

                int depth = SchedulerProbe.connectionTimerQueueDepth();
                assertTrue(
                        depth <= baseline + 8,
                        "Timer queue grew while reading: baseline=" + baseline + " observed=" + depth
                                + ". The inactivity timeout is rescheduled per chunk and cancelled "
                                + "tasks are never evicted.");
            } finally {
                connection.close();
            }
        }
    }
}
