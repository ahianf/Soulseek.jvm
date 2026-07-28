// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.soak;

import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationSignal;
import dev.slsk.internal.network.tcp.SocketConnection;
import dev.slsk.internal.options.ConnectionOptions;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Scenario: user listeners that block.
 *
 * <p>{@code SocketConnection.disconnect} holds {@code synchronized(this)} while
 * {@code changeState} invokes every listener inline. A listener that blocks
 * therefore holds a library monitor, which is defect 1.8. On Java 21 that also
 * pins a carrier thread; JEP 491 removes the pinning half of the problem on 25,
 * but the deadlock exposure remains until the lock is released before the
 * callback runs.
 *
 * <p>This scenario asserts the library stays live when user code misbehaves.
 */
class BlockingListenerSoak {

    private static final int CONNECTIONS = 32;
    private static final long LISTENER_BLOCK_MILLIS = 250;

    @Test
    @DisplayName("Blocking listeners do not deadlock disconnect")
    void blockingListenersDoNotDeadlock() throws Exception {
        try (LoopbackPeer peer = LoopbackPeer.start(LoopbackPeer.Behaviour.IDLE, 256)) {
            AtomicInteger stateEvents = new AtomicInteger();
            CountDownLatch allDisconnected = new CountDownLatch(CONNECTIONS);

            SocketConnection[] connections = new SocketConnection[CONNECTIONS];
            for (int index = 0; index < CONNECTIONS; index++) {
                SocketConnection connection = new SocketConnection(peer.endpoint(), new ConnectionOptions());
                connection.addStateChangedListener((sender, event) -> {
                    stateEvents.incrementAndGet();
                    sleepQuietly(LISTENER_BLOCK_MILLIS);
                });
                connection.addDisconnectedListener((sender, event) -> allDisconnected.countDown());
                connection.connectAsync(CancellationSignal.none()).join();
                connections[index] = connection;
            }

            long start = System.nanoTime();
            // Close from many threads at once: if the monitor is held across a
            // blocking callback, contention here is what surfaces it.
            Thread[] closers = new Thread[CONNECTIONS];
            for (int index = 0; index < CONNECTIONS; index++) {
                SocketConnection connection = connections[index];
                closers[index] = Thread.ofVirtual().start(connection::close);
            }
            for (Thread closer : closers) {
                closer.join(TimeUnit.SECONDS.toMillis(60));
            }

            boolean settled = allDisconnected.await(60, TimeUnit.SECONDS);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            SoakReport.record("blocking-listener", "connections", CONNECTIONS);
            SoakReport.record("blocking-listener", "listener block per event", LISTENER_BLOCK_MILLIS + " ms");
            SoakReport.record("blocking-listener", "state events observed", stateEvents.get());
            SoakReport.record("blocking-listener", "total teardown", elapsedMillis + " ms");
            SoakReport.record(
                    "blocking-listener",
                    "serialised?",
                    elapsedMillis > CONNECTIONS * LISTENER_BLOCK_MILLIS ? "yes" : "no");

            assertTrue(settled, "Not every connection disconnected; a blocking listener deadlocked teardown.");
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}
