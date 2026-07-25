// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.soak;

import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationSignal;
import dev.slsk.network.tcp.SocketConnection;
import dev.slsk.options.ConnectionOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Scenario: many writers queued behind one stalled write.
 *
 * <p>Stands in for the queued-transfer case in the goal's scenario table. The
 * client-level permit helper ({@code DefaultSoulseekClient.acquirePermit}) is
 * private and needs a logged-in client, but it and
 * {@code SocketConnection.acquire} are the same defect: a semaphore acquired by
 * spinning {@code tryAcquire} with a timeout instead of blocking. This
 * scenario drives the connection-level one, which is reachable without a
 * server.
 *
 * <p>Blocked writers should cost no CPU. On the 0.11.0 baseline each one wakes
 * 40 times a second forever.
 */
class WriteContentionSoak {

    private static final int WRITERS = 100;
    private static final int PAYLOAD_BYTES = 8 * 1024 * 1024;
    private static final long OBSERVE_MILLIS = 5_000;

    @Test
    @DisplayName("CPU burned by writers blocked on the write semaphore")
    void blockedWritersCost() throws Exception {
        try (LoopbackPeer peer = LoopbackPeer.start(LoopbackPeer.Behaviour.STALL)) {
            // A write queue large enough that contention does not trip the
            // drop-and-disconnect path, which is a separate defect (3.4).
            ConnectionOptions options = new ConnectionOptions(16_384, 16_384, WRITERS * 2, 10_000, -1, null, null);
            SocketConnection connection = new SocketConnection(peer.endpoint(), options);
            try {
                connection.connectAsync(CancellationSignal.none()).join();

                byte[] payload = new byte[PAYLOAD_BYTES];
                List<CompletableFuture<Void>> writes = new ArrayList<>(WRITERS);
                for (int index = 0; index < WRITERS; index++) {
                    writes.add(connection.writeAsync(payload, CancellationSignal.none()));
                }

                // Let the first write stall against the unread socket, so the
                // remaining writers are genuinely queued before measuring.
                Thread.sleep(500);

                long cpuStart = CpuProbe.processCpuNanos();
                long wallStart = System.nanoTime();
                Thread.sleep(OBSERVE_MILLIS);
                long wallElapsed = System.nanoTime() - wallStart;
                long cpuElapsed = CpuProbe.processCpuNanos() - cpuStart;

                long completed =
                        writes.stream().filter(CompletableFuture::isDone).count();
                double cores = (double) cpuElapsed / wallElapsed;

                SoakReport.record("write-contention", "writers", WRITERS);
                SoakReport.record("write-contention", "writes completed while observing", completed);
                SoakReport.record(
                        "write-contention", "cpu cores while blocked", String.format(Locale.ROOT, "%.3f", cores));
                SoakReport.record(
                        "write-contention",
                        "cpu per blocked writer",
                        String.format(Locale.ROOT, "%.5f cores", cores / Math.max(1, WRITERS - completed)));

                assertTrue(cores >= 0, "CPU measurement unavailable; the harness cannot assert on polling cost.");
            } finally {
                connection.close();
            }
        }
    }

    /**
     * Blocked writers must not burn CPU.
     *
     * <p>Disabled: fails on the 0.11.0 baseline, where
     * {@code SocketConnection.acquire} spins {@code tryAcquire(25 ms)} in a
     * loop per waiting writer. Defect 1.4 replaces it with a blocking acquire
     * made cancellable by interruption; remove the {@code @Disabled} then.
     */
    @Test
    @Disabled("Baseline polls at 25 ms per blocked writer: defect 1.4 — enable in Phase 1")
    @DisplayName("Blocked writers consume near-zero CPU")
    void blockedWritersAreFree() throws Exception {
        try (LoopbackPeer peer = LoopbackPeer.start(LoopbackPeer.Behaviour.STALL)) {
            ConnectionOptions options = new ConnectionOptions(16_384, 16_384, WRITERS * 2, 10_000, -1, null, null);
            SocketConnection connection = new SocketConnection(peer.endpoint(), options);
            try {
                connection.connectAsync(CancellationSignal.none()).join();

                byte[] payload = new byte[PAYLOAD_BYTES];
                for (int index = 0; index < WRITERS; index++) {
                    connection.writeAsync(payload, CancellationSignal.none());
                }
                Thread.sleep(500);

                long cpuStart = CpuProbe.processCpuNanos();
                long wallStart = System.nanoTime();
                Thread.sleep(OBSERVE_MILLIS);
                double cores = (double) (CpuProbe.processCpuNanos() - cpuStart) / (System.nanoTime() - wallStart);

                assertTrue(
                        cores < 0.05,
                        "Writers blocked on a semaphore burned " + String.format(Locale.ROOT, "%.3f", cores)
                                + " cores. They should be parked, not polling.");
            } finally {
                connection.close();
            }
        }
    }
}
