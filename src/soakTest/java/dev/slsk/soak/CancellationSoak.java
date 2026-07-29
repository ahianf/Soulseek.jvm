// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.soak;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationController;
import dev.slsk.internal.network.tcp.ConnectionState;
import dev.slsk.internal.network.tcp.SocketConnection;
import dev.slsk.internal.options.ConnectionOptions;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Scenario: cancel a transfer while it is running.
 *
 * <p>The 0.11.0 baseline aborted promptly only because
 * {@code NetworkStreamAdapter.observeCancellation} closed the socket from the
 * cancelling thread, underneath a reader blocked mid-call. With a silent peer
 * that was the <em>only</em> way the read could ever wake, since the socket
 * timeout was the 15-second inactivity budget.
 *
 * <p>Phase 2 removed that cross-thread close. The reader now wakes on its own
 * within a bounded poll window and decides to abort in order. The cancelled
 * connection still disconnects, which is correct: a read abandoned partway
 * leaves the stream position indeterminate, and for a framed connection the
 * next read would resume mid-frame. What must not happen is one cancellation
 * disturbing anything else.
 */
class CancellationSoak {

    /**
     * Where a read that is going to be raced against a cancellation runs.
     *
     * <p>The reads here are blocking, so racing one needs a thread; the futures
     * these used to be existed only because the read did.
     */
    private static final java.util.concurrent.Executor READERS =
            task -> Thread.ofVirtual().start(task);

    private static CompletableFuture<Void> reader(Runnable read) {
        return CompletableFuture.runAsync(read, READERS);
    }

    private static final long TRANSFER_BYTES = 4L * 1024 * 1024 * 1024;

    @Test
    @DisplayName("Cancellation aborts the transfer promptly")
    void cancellationAbortsPromptly() throws Exception {
        try (LoopbackPeer peer =
                LoopbackPeer.start(LoopbackPeer.Behaviour.BYTE_SOURCE).withByteSourceLength(TRANSFER_BYTES)) {

            SocketConnection connection = new SocketConnection(peer.endpoint(), new ConnectionOptions());
            try {
                connection.connect(dev.slsk.CancellationSignal.none());

                CancellationController controller = new CancellationController();
                CompletableFuture<Void> transfer = reader(() -> connection.read(
                        TRANSFER_BYTES, OutputStream.nullOutputStream(), null, null, controller.getSignal()));

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
     * A read blocked against a silent peer must still abort promptly, without
     * another thread closing the socket underneath it.
     *
     * <p>This is the case the baseline could not handle on its own: with no
     * data arriving and the socket timeout set to the 15-second inactivity
     * budget, the reader was parked indefinitely and only the cross-thread
     * {@code socket.close()} could wake it. The bounded cancellation poll makes
     * the reader responsible for noticing.
     */
    @Test
    @DisplayName("Cancellation aborts a read from a silent peer")
    void cancellationAbortsAgainstSilentPeer() throws Exception {
        try (LoopbackPeer peer = LoopbackPeer.start(LoopbackPeer.Behaviour.IDLE)) {
            SocketConnection connection = new SocketConnection(peer.endpoint(), new ConnectionOptions());
            try {
                connection.connect(dev.slsk.CancellationSignal.none());

                CancellationController controller = new CancellationController();
                CompletableFuture<Void> read = reader(() -> connection.read(
                        TRANSFER_BYTES, OutputStream.nullOutputStream(), null, null, controller.getSignal()));

                // Long enough that the reader is parked in the socket read with
                // nothing to return.
                Thread.sleep(400);

                long start = System.nanoTime();
                controller.cancel();
                assertThrows(RuntimeException.class, read::join);
                long abortMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

                SoakReport.record("cancellation", "silent-peer abort latency", abortMillis + " ms");
                assertTrue(abortMillis < 2_000, "A read against a silent peer took " + abortMillis + " ms to abort.");
            } finally {
                connection.close();
            }
        }
    }

    /**
     * Cancelling one connection must not disturb another.
     *
     * <p>The cancelled connection does disconnect, which is correct — its
     * stream position is indeterminate after an abandoned read. The property
     * worth guaranteeing is isolation, and the baseline's cross-thread
     * {@code socket.close()} is exactly the kind of mechanism that threatens it.
     */
    @Test
    @DisplayName("Cancelling one connection leaves others untouched")
    void cancellationIsIsolatedToItsConnection() throws Exception {
        try (LoopbackPeer peer =
                LoopbackPeer.start(LoopbackPeer.Behaviour.BYTE_SOURCE).withByteSourceLength(TRANSFER_BYTES)) {

            SocketConnection cancelled = new SocketConnection(peer.endpoint(), new ConnectionOptions());
            SocketConnection bystander = new SocketConnection(peer.endpoint(), new ConnectionOptions());
            try {
                cancelled.connect(dev.slsk.CancellationSignal.none());
                bystander.connect(dev.slsk.CancellationSignal.none());

                CancellationController controller = new CancellationController();
                CompletableFuture<Void> doomed = reader(() -> cancelled.read(
                        TRANSFER_BYTES, OutputStream.nullOutputStream(), null, null, controller.getSignal()));
                CompletableFuture<byte[]> survivor = CompletableFuture.supplyAsync(
                        () -> bystander.read(64, dev.slsk.CancellationSignal.none()), READERS);

                Thread.sleep(250);
                controller.cancel();
                assertThrows(RuntimeException.class, doomed::join);

                assertEquals(
                        ConnectionState.DISCONNECTED,
                        cancelled.getState(),
                        "The cancelled connection's stream position is indeterminate; it must disconnect.");
                assertEquals(64, survivor.join().length, "The bystander's read did not complete.");
                assertEquals(
                        ConnectionState.CONNECTED,
                        bystander.getState(),
                        "Cancelling one connection disturbed another.");
            } finally {
                cancelled.close();
                bystander.close();
            }
        }
    }
}
