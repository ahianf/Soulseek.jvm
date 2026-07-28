// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.soak;

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
 */
public final class SchedulerProbe {

    private static final String TIMER_OWNER = "dev.slsk.internal.network.tcp.SocketConnection";
    private static final String TIMER_FIELD = "TIMER_EXECUTOR";

    private SchedulerProbe() {}

    /**
     * Returns the number of tasks resident in the shared connection timer
     * queue, including cancelled-but-not-yet-evicted ones.
     *
     * @throws IllegalStateException if the probe target has moved
     */
    public static int connectionTimerQueueDepth() {
        return queueDepth(TIMER_OWNER, TIMER_FIELD);
    }

    /**
     * Returns whether the shared connection timer evicts cancelled tasks
     * immediately. False is the defect; true is the fixed state.
     */
    public static boolean connectionTimerRemovesOnCancel() {
        return executor(TIMER_OWNER, TIMER_FIELD).getRemoveOnCancelPolicy();
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

    private static int queueDepth(String className, String fieldName) {
        return executor(className, fieldName).getQueue().size();
    }

    private static ScheduledThreadPoolExecutor executor(String className, String fieldName) {
        Object value;
        try {
            Class<?> owner = Class.forName(className);
            Field field = owner.getDeclaredField(fieldName);
            field.setAccessible(true);
            value = field.get(null);
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException(
                    "SchedulerProbe target " + className + "." + fieldName
                            + " has moved. Update SchedulerProbe in the same commit that moved it, "
                            + "so the soak assertion keeps measuring something real.",
                    exception);
        }
        if (value instanceof ScheduledThreadPoolExecutor executor) {
            return executor;
        }
        throw new IllegalStateException(className + "." + fieldName
                + " is no longer a ScheduledThreadPoolExecutor but a "
                + (value == null ? "null" : value.getClass().getName())
                + "; update SchedulerProbe to match.");
    }
}
