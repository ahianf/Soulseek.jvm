// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.soak;

import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.common.Monitors;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.network.tcp.SocketConnection;
import dev.slsk.internal.options.ConnectionOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Scenario: N idle connections, for N = 1, 10, 100, 1000.
 *
 * <p>Asserts that connection count drives virtual-thread count only. Platform
 * threads and scheduled tasks must both stay O(1). This is the load-bearing
 * claim of the whole fork: an idle connection should cost almost nothing.
 */
class IdleConnectionScalingSoak {

    private static final int[] SCALES = {1, 10, 100, 1000};

    /**
     * Sweeps every scale once, recording both metrics, and asserts the
     * invariant that already holds on the 0.11.0 baseline: connection I/O runs
     * on virtual threads, so platform threads stay flat.
     *
     * <p>The scheduled-task numbers are recorded here rather than in
     * {@link #scheduledTaskCountIsConstant()} so that Phase 0 captures a
     * baseline even while that assertion is still failing.
     */
    @Test
    @DisplayName("Platform thread count is O(1) in connection count")
    void platformThreadCountIsConstant() throws Exception {
        try (LoopbackPeer peer = LoopbackPeer.start(LoopbackPeer.Behaviour.IDLE, 2048)) {
            int threadBaseline = ThreadCensus.libraryThreadCount();
            int queueBaseline = SchedulerProbe.connectionTimerQueueDepth();
            SoakReport.record("idle-scaling", "library platform threads at rest", threadBaseline);
            SoakReport.record("idle-scaling", "timer queue depth at rest", queueBaseline);
            SoakReport.record(
                    "idle-scaling", "timer removeOnCancelPolicy", SchedulerProbe.connectionTimerRemovesOnCancel());
            // The connections these tests open have no client, so they sweep on
            // the harness's own scheduler and its platform thread is outside the
            // library census by name. A real client's sweep runs on its
            // soulseek-client-timer, which client-lifecycle counts and prices at
            // 1.0 platform threads per client. The two this used to report were
            // the static soulseek-connection-timer pool, which no longer exists.
            SoakReport.note(
                    "idle-scaling",
                    "connections sweep on the harness scheduler (1 platform thread, outside the census); "
                            + "a client's sweep is on its own soulseek-client-timer");

            for (int scale : SCALES) {
                List<SocketConnection> connections = openConnections(peer, scale);
                try {
                    int threads = ThreadCensus.libraryThreadCount();
                    SoakReport.record("idle-scaling", "platform threads @ " + scale + " conns", threads);
                    SoakReport.record(
                            "idle-scaling",
                            "timer queue depth @ " + scale + " conns",
                            SchedulerProbe.connectionTimerQueueDepth());

                    assertTrue(
                            threads <= threadBaseline + 4,
                            "Platform threads grew with connection count at scale " + scale
                                    + ": baseline=" + threadBaseline + " observed=" + threads
                                    + " census=" + ThreadCensus.describe());
                } finally {
                    closeAll(connections);
                }

                // How long the queue takes to drain after every connection is
                // closed is the direct symptom of removeOnCancelPolicy=false:
                // cancelled tasks sit until their original deadline elapses.
                long start = System.nanoTime();
                int drained = SchedulerProbe.awaitConnectionTimerQueueAtMost(queueBaseline + 8, 20, TimeUnit.SECONDS);
                SoakReport.record(
                        "idle-scaling",
                        "queue drain after close @ " + scale + " conns",
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " ms -> depth " + drained);
            }
        }
    }

    /**
     * Scheduled tasks must not scale with connection count.
     *
     * <p>The 0.11.0 baseline was O(N) at roughly 3 tasks per connection
     * (3 / 33 / 300 / 3,000 at the four scales): each connection installed its
     * own fixed-rate watchdog plus a per-chunk inactivity timeout, into an
     * executor that never evicted cancelled tasks. Defect 1.2 removed the
     * per-chunk churn and 1.3 folded the per-connection watchdog into one
     * shared sweep.
     */
    @Test
    @DisplayName("Scheduled task count is O(1) in connection count")
    void scheduledTaskCountIsConstant() throws Exception {
        try (LoopbackPeer peer = LoopbackPeer.start(LoopbackPeer.Behaviour.IDLE, 2048)) {
            int baseline = SchedulerProbe.connectionTimerQueueDepth();

            int worst = baseline;
            for (int scale : SCALES) {
                List<SocketConnection> connections = openConnections(peer, scale);
                try {
                    worst = Math.max(worst, SchedulerProbe.connectionTimerQueueDepth());
                } finally {
                    closeAll(connections);
                }
                SchedulerProbe.awaitConnectionTimerQueueAtMost(baseline + 8, 20, TimeUnit.SECONDS);
            }

            assertTrue(
                    worst <= baseline + 16,
                    "Scheduled tasks grew with connection count: baseline=" + baseline
                            + " worst=" + worst
                            + ". Each SocketConnection installs its own fixed-rate watchdog; "
                            + "defects 1.2 and 1.3 fold these into one per-client sweep.");
        }
    }

    private static List<SocketConnection> openConnections(LoopbackPeer peer, int count) throws Exception {
        // Timers left at source defaults: the point of the scenario is to
        // measure what the defaults cost per connection.
        ConnectionOptions options = new ConnectionOptions();
        List<SocketConnection> connections = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            SocketConnection connection = new SocketConnection(peer.endpoint(), options, null, Monitors.shared());
            connection.connect(CancellationSignal.none());
            connections.add(connection);
        }
        return connections;
    }

    private static void closeAll(List<SocketConnection> connections) {
        for (SocketConnection connection : connections) {
            try {
                connection.close();
            } catch (RuntimeException ignored) {
                // Teardown is best effort; the assertions already ran.
            }
        }
    }
}
