// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.common;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A timer wheel backed by one platform thread.
 *
 * <p>The library used to create a single-thread scheduler per component: one
 * for the waiter, one for each token bucket, one for client cleanup, one for
 * distributed status, and — worst — one per active search. That was four
 * platform threads per idle client before a single search ran.
 *
 * <p>None of those threads did any work. They waited on a delay queue and then
 * ran a callback. One thread can do that for the whole client, provided it only
 * ever <em>dispatches</em>: every scheduled task is handed to a virtual thread
 * to run, so a task that blocks cannot stall the timer and every other task
 * behind it. {@code DefaultWaiter} already did this for its timeouts and its
 * comment gave the reason; this generalises it.
 *
 * <p>Instances are per-client, not static, so two clients in one JVM stay
 * independent and closing a client releases its thread.
 */
public final class Scheduler implements AutoCloseable {

    private final ScheduledExecutorService timer;
    private final ExecutorService worker;

    /**
     * Creates a scheduler.
     *
     * @param name the timer thread name
     */
    public Scheduler(String name) {
        Objects.requireNonNull(name, "name");
        timer = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        });
        // Cancelled one-shots would otherwise sit in the delay queue until
        // their original deadline; see defect 1.2 for what that costs.
        if (timer instanceof java.util.concurrent.ScheduledThreadPoolExecutor pool) {
            pool.setRemoveOnCancelPolicy(true);
        }
        worker = NetworkExecutor.executor();
    }

    /**
     * Runs a task once after a delay.
     *
     * @param task the task
     * @param delay the delay
     * @param unit the delay unit
     * @return a handle that cancels the pending dispatch
     */
    public ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
        Objects.requireNonNull(task, "task");
        return timer.schedule(() -> worker.execute(task), delay, unit);
    }

    /**
     * Runs a task repeatedly.
     *
     * <p>Runs do not overlap. If one is still going when the next is due, the
     * next is skipped rather than queued, so a slow task cannot accumulate an
     * unbounded backlog of virtual threads.
     *
     * @param task the task
     * @param initialDelay the delay before the first run
     * @param period the period between runs
     * @param unit the time unit
     * @return a handle that cancels the repeating dispatch
     */
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long initialDelay, long period, TimeUnit unit) {
        Objects.requireNonNull(task, "task");
        AtomicBoolean running = new AtomicBoolean();
        return timer.scheduleAtFixedRate(
                () -> {
                    if (!running.compareAndSet(false, true)) {
                        return;
                    }
                    worker.execute(() -> {
                        try {
                            task.run();
                        } finally {
                            running.set(false);
                        }
                    });
                },
                initialDelay,
                period,
                unit);
    }

    /** Stops the timer and releases its thread. Dispatched tasks are not awaited. */
    @Override
    public void close() {
        timer.shutdownNow();
    }
}
