// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Sweeps every registered connection from one scheduled task.
 *
 * <p>Each connection previously installed its own fixed-rate task, so the
 * shared timer queue held one entry per open connection: measured at 3,000
 * entries for 1,000 idle connections. Liveness and inactivity are cheap
 * per-connection checks with no ordering requirement between connections, so
 * they do not need a task each.
 *
 * <p>The sweep runs at the shortest cadence any registered connection asks for,
 * recomputed during the sweep itself so it costs nothing extra. That keeps a
 * connection with a 20 ms inactivity timeout as precise as it was while letting
 * a thousand connections at the 15-second default share a single 250 ms task.
 */
final class ConnectionMonitor {

    private final ScheduledExecutorService scheduler;
    private final Set<SocketConnection> connections = ConcurrentHashMap.newKeySet();
    private final Object lock = new Object();

    private ScheduledFuture<?> sweepTask;
    private int sweepIntervalMillis;

    ConnectionMonitor(ScheduledExecutorService scheduler) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    /** Adds a connection to the sweep, starting or quickening it if needed. */
    void register(SocketConnection connection) {
        connections.add(connection);

        int desired = connection.monitorIntervalMillis();
        synchronized (lock) {
            if (sweepTask == null || desired < sweepIntervalMillis) {
                restartSweep(desired);
            }
        }
    }

    /**
     * Removes a connection from the sweep.
     *
     * <p>The cadence is not recomputed here. Sweeping faster than necessary is
     * harmless and the next sweep corrects it; recomputing on every removal
     * would make tearing down N connections O(N^2).
     */
    void unregister(SocketConnection connection) {
        connections.remove(connection);
    }

    /** Returns the number of registered connections, for tests. */
    int registeredCount() {
        return connections.size();
    }

    /** Returns the current sweep cadence in milliseconds, or 0 when idle. */
    int sweepIntervalMillis() {
        synchronized (lock) {
            return sweepTask == null ? 0 : sweepIntervalMillis;
        }
    }

    private void sweep() {
        int shortest = Integer.MAX_VALUE;

        for (SocketConnection connection : connections) {
            try {
                connection.monitorTick();
            } catch (RuntimeException exception) {
                // One misbehaving connection must not stop the others from
                // being swept. The connection's own error handling has already
                // run by this point.
            }
            shortest = Math.min(shortest, connection.monitorIntervalMillis());
        }

        synchronized (lock) {
            if (connections.isEmpty()) {
                stopSweep();
            } else if (shortest != Integer.MAX_VALUE && shortest != sweepIntervalMillis) {
                restartSweep(shortest);
            }
        }
    }

    private void restartSweep(int intervalMillis) {
        // Cancelling from inside the sweep is safe: cancel(false) stops future
        // runs without interrupting the one in progress.
        if (sweepTask != null) {
            sweepTask.cancel(false);
        }
        sweepIntervalMillis = intervalMillis;
        sweepTask = scheduler.scheduleAtFixedRate(this::sweep, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void stopSweep() {
        if (sweepTask != null) {
            sweepTask.cancel(false);
            sweepTask = null;
        }
        sweepIntervalMillis = 0;
    }
}
