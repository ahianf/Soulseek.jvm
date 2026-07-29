// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.internal.network.tcp.ConnectionMonitor;

/**
 * The connection monitor the transport tests sweep on.
 *
 * <p>Every connection now names whose sweep it belongs to, because a client's
 * connections are that client's to watch and the sweep used to run on a static
 * pool shared by the whole JVM. A test has no client, so it says so here: one
 * monitor for the test run, on a scheduler that dies with it.
 *
 * <p>This is the shape the production code no longer has, kept deliberately in
 * one visible place rather than reintroduced as a default nobody passes.
 */
public final class Monitors {

    private static final Scheduler SCHEDULER = new Scheduler("test-connection-monitor");
    private static final ConnectionMonitor MONITOR = new ConnectionMonitor(SCHEDULER);

    private Monitors() {}

    /**
     * Returns the shared test monitor.
     *
     * @return the monitor
     */
    public static ConnectionMonitor shared() {
        return MONITOR;
    }

    /**
     * Returns the scheduler the shared monitor sweeps on.
     *
     * <p>The soak harness measures its timer queue, which is what
     * {@code SocketConnection.TIMER_EXECUTOR} used to be.
     *
     * @return the scheduler
     */
    public static Scheduler scheduler() {
        return SCHEDULER;
    }
}
