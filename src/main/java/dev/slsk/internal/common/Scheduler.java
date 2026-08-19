// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import java.util.Objects;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
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

    private final ScheduledThreadPoolExecutor timer;
    private final ExecutorService worker;

    /**
     * Creates a scheduler.
     *
     * @param name the timer thread name
     */
    public Scheduler(String name) {
        Objects.requireNonNull(name, "name");
        // Constructed directly rather than through
        // Executors.newSingleThreadScheduledExecutor, which hands back a
        // DelegatedScheduledExecutorService wrapper. The instanceof guard that
        // used to set the policy below therefore never matched, and the policy
        // was never applied: cancelled one-shots sat in the delay queue until
        // their original deadline, which is the cost defect 1.2 named.
        timer = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        });
        timer.setRemoveOnCancelPolicy(true);
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
        try {
            return timer.schedule(() -> worker.execute(task), delay, unit);
        } catch (RejectedExecutionException closed) {
            return CLOSED;
        }
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
        // Built once here rather than inside the tick: at the 250 ms monitor
        // cadence the per-tick capture was a steady allocation for a lambda
        // that never changes.
        Runnable dispatch = () -> {
            try {
                task.run();
            } finally {
                running.set(false);
            }
        };
        try {
            return timer.scheduleAtFixedRate(
                    () -> {
                        if (!running.compareAndSet(false, true)) {
                            return;
                        }
                        worker.execute(dispatch);
                    },
                    initialDelay,
                    period,
                    unit);
        } catch (RejectedExecutionException closed) {
            return CLOSED;
        }
    }

    /** Stops the timer and releases its thread. Dispatched tasks are not awaited. */
    @Override
    public void close() {
        timer.shutdownNow();
    }

    /**
     * What a closed scheduler hands back instead of throwing.
     *
     * <p>Shutting down is a state, not a failure — the same rule the read loop
     * follows for its own stop. Everything scheduled here is housekeeping a
     * client does for itself: a download retry, a status debounce, a connection
     * sweep, a search timeout. A client that has closed has nothing left to do
     * any of it for, so the schedule is dropped rather than raised.
     *
     * <p>It mattered as soon as the futures came out. A retry scheduled during
     * shutdown used to be swallowed by the {@code CompletableFuture} its caller
     * discarded; with the dispatch reporting its own failures the same rejection
     * reached the uncaught-exception handler, which is where a normal close must
     * print nothing.
     */
    private static final ScheduledFuture<?> CLOSED = new ScheduledFuture<Object>() {
        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(Delayed other) {
            return other == this ? 0 : -1;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return true;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Object get() {
            return null;
        }

        @Override
        public Object get(long timeout, TimeUnit unit) {
            return null;
        }
    };
}
