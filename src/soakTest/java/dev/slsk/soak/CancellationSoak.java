// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.soak;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationController;
import dev.slsk.network.tcp.ConnectionState;
import dev.slsk.network.tcp.SocketConnection;
import dev.slsk.options.ConnectionOptions;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Scenario: cancel a transfer while it is running.
 *
 * <p>Two properties matter. The transfer must abort promptly, which the 0.11.0
 * baseline already does. And the connection carrying it must survive, which the
 * baseline does not do: {@code NetworkStreamAdapter.observeCancellation} closes
 * the socket because a blocking stream read cannot otherwise be interrupted.
 * Its own comment concedes this.
 *
 * <p>Phase 2 replaces that with thread interruption, at which point cancelling
 * one transfer stops killing the connection it shares with everything else.
 */
class CancellationSoak {

    private static final long TRANSFER_BYTES = 4L * 1024 * 1024 * 1024;

    @Test
    @DisplayName("Cancellation aborts the transfer promptly")
    void cancellationAbortsPromptly() throws Exception {
        try (LoopbackPeer peer =
                LoopbackPeer.start(LoopbackPeer.Behaviour.BYTE_SOURCE).withByteSourceLength(TRANSFER_BYTES)) {

            SocketConnection connection = new SocketConnection(peer.endpoint(), new ConnectionOptions());
            try {
                connection.connectAsync(dev.slsk.CancellationSignal.none()).join();

                CancellationController controller = new CancellationController();
                CompletableFuture<Void> transfer = connection.readAsync(
                        TRANSFER_BYTES, OutputStream.nullOutputStream(), null, null, controller.getSignal());

                Thread.sleep(250);

                long start = System.nanoTime();
                controller.cancel();
                // join() rethrows CancellationException unwrapped and wraps
                // everything else in CompletionException, so both are valid
                // shapes for an aborted read.
                assertThrows(RuntimeException.class, transfer::join, "A cancelled read must not complete normally.");
                long abortMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

                SoakReport.record("cancellation", "abort latency", abortMillis + " ms");
                SoakReport.record("cancellation", "connection state after cancel", connection.getState());

                assertTrue(abortMillis < 2_000, "Cancellation took " + abortMillis + " ms to take effect.");
            } finally {
                connection.close();
            }
        }
    }

    /**
     * Cancelling a transfer must not tear down its connection.
     *
     * <p>Disabled: fails on the 0.11.0 baseline by construction. Cancellation
     * is implemented by closing the socket, so the connection is always
     * DISCONNECTED afterwards. Phase 2 (goal 2.2) makes cancellation an
     * interrupt and leaves the connection usable; remove the {@code @Disabled}
     * in that commit.
     *
     * <p>This is a deliberate behavioural divergence from Soulseek.NET and is
     * recorded in {@code docs/fork-divergence.md}.
     */
    @Test
    @Disabled("Baseline cancels by closing the socket: goal 2.2 — enable in Phase 2")
    @DisplayName("Connection survives a cancelled transfer")
    void connectionSurvivesCancellation() throws Exception {
        try (LoopbackPeer peer =
                LoopbackPeer.start(LoopbackPeer.Behaviour.BYTE_SOURCE).withByteSourceLength(TRANSFER_BYTES)) {

            SocketConnection connection = new SocketConnection(peer.endpoint(), new ConnectionOptions());
            try {
                connection.connectAsync(dev.slsk.CancellationSignal.none()).join();

                CancellationController controller = new CancellationController();
                CompletableFuture<Void> transfer = connection.readAsync(
                        TRANSFER_BYTES, OutputStream.nullOutputStream(), null, null, controller.getSignal());

                Thread.sleep(250);
                controller.cancel();
                assertThrows(RuntimeException.class, transfer::join);

                assertEquals(
                        ConnectionState.CONNECTED,
                        connection.getState(),
                        "Cancelling one transfer must not disconnect the connection carrying it.");
            } finally {
                connection.close();
            }
        }
    }
}
