// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.soak;

import dev.slsk.internal.common.Monitors;
import dev.slsk.internal.common.Scheduler;
import java.lang.reflect.Field;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Reads the depth of the library's internal scheduled-task queues.
 *
 * <p>Defect 1.2 in {@code JAVA_FORK_ARCHITECTURE_GOAL.md} is that the
 * inactivity timeout is cancelled and rescheduled on every buffer chunk, and
 * that the executor is created without {@code setRemoveOnCancelPolicy(true)},
 * so cancelled tasks stay resident in the delay queue until their deadline
 * elapses. Queue depth is the only direct measurement of that, and it is not
 * reachable through any public API.
 *
 * <p>This probe therefore reflects into a known private field. It is
 * deliberately brittle: if the field moves, {@link #connectionTimerQueueDepth()}
 * throws rather than quietly returning zero. A silent probe would let a
 * refactor turn a real assertion into a no-op, which is the specific failure
 * mode this harness exists to prevent. When a phase relocates the scheduler,
 * update the constants below in the same commit.
 *
 * <p>Phase 6 relocated it. The queue was {@code SocketConnection.TIMER_EXECUTOR},
 * a static two-thread platform pool shared by every client in the JVM; it is now
 * the timer of whichever {@link Scheduler} a client's {@code ConnectionMonitor}
 * sweeps on. The soak harness has no client, so it measures the scheduler behind
 * {@link Monitors}, which is the one every connection these tests open is swept
 * by — the same quantity, read off an instance rather than a static.
 */
public final class SchedulerProbe {

    private static final String TIMER_OWNER = "dev.slsk.internal.common.Scheduler";
    private static final String TIMER_FIELD = "timer";

    private SchedulerProbe() {}

    /**
     * Returns the number of tasks resident in the shared connection timer
     * queue, including cancelled-but-not-yet-evicted ones.
     *
     * @throws IllegalStateException if the probe target has moved
     */
    public static int connectionTimerQueueDepth() {
        return executor(Monitors.scheduler()).getQueue().size();
    }

    /**
     * Returns whether the shared connection timer evicts cancelled tasks
     * immediately. False is the defect; true is the fixed state.
     */
    public static boolean connectionTimerRemovesOnCancel() {
        return executor(Monitors.scheduler()).getRemoveOnCancelPolicy();
    }

    /**
     * Waits for the connection timer queue to fall to or below a bound.
     *
     * @return the final depth, whether or not it met the bound
     */
    public static int awaitConnectionTimerQueueAtMost(int bound, long timeout, TimeUnit unit)
            throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        int depth = connectionTimerQueueDepth();
        while (depth > bound && System.nanoTime() < deadline) {
            Thread.sleep(25);
            depth = connectionTimerQueueDepth();
        }
        return depth;
    }

    private static ScheduledThreadPoolExecutor executor(Scheduler scheduler) {
        Object value;
        try {
            Field field = Scheduler.class.getDeclaredField(TIMER_FIELD);
            field.setAccessible(true);
            value = field.get(scheduler);
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException(
                    "SchedulerProbe target " + TIMER_OWNER + "." + TIMER_FIELD
                            + " has moved. Update SchedulerProbe in the same commit that moved it, "
                            + "so the soak assertion keeps measuring something real.",
                    exception);
        }
        if (value instanceof ScheduledThreadPoolExecutor executor) {
            return executor;
        }
        throw new IllegalStateException(TIMER_OWNER + "." + TIMER_FIELD
                + " is no longer a ScheduledThreadPoolExecutor but a "
                + (value == null ? "null" : value.getClass().getName())
                + "; update SchedulerProbe to match.");
    }
}
