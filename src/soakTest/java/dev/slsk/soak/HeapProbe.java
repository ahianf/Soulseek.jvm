// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.soak;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Locale;

/**
 * Measures heap residency and cumulative allocation.
 *
 * <p>Two different questions need two different measurements. "Does the heap
 * grow without bound?" is answered by live-set size after a collection.
 * "Is the hot path allocating more than it should?" is answered by cumulative
 * allocated bytes, which counts garbage that a collection has already removed
 * and which the live set therefore cannot see.
 */
public final class HeapProbe {

    private static final MemoryMXBean MEMORY = ManagementFactory.getMemoryMXBean();
    private static final ThreadMXBean THREADS = ManagementFactory.getThreadMXBean();

    private HeapProbe() {}

    /**
     * Returns used heap in bytes after attempting a collection.
     *
     * <p>{@code System.gc()} is a request, not a command, so this runs two
     * rounds with a short settle between them. Scenarios must assert on
     * generous bounds and on growth across iterations rather than on absolute
     * values.
     */
    public static long liveHeapBytes() throws InterruptedException {
        for (int round = 0; round < 2; round++) {
            System.gc();
            Thread.sleep(50);
        }
        return MEMORY.getHeapMemoryUsage().getUsed();
    }

    /** Returns used heap in bytes without forcing a collection. */
    public static long usedHeapBytes() {
        return MEMORY.getHeapMemoryUsage().getUsed();
    }

    /**
     * Returns cumulative bytes allocated across all threads since JVM start,
     * or {@code -1} when the JVM does not support the measurement.
     *
     * <p>This counts allocation by virtual threads too, which is the point:
     * the thread-amplification defect shows up here as garbage even when the
     * live set stays flat.
     */
    public static long totalAllocatedBytes() {
        if (THREADS instanceof com.sun.management.ThreadMXBean sunThreads
                && sunThreads.isThreadAllocatedMemorySupported()) {
            return sunThreads.getTotalThreadAllocatedBytes();
        }
        return -1;
    }

    /** Returns whether cumulative allocation measurement is available. */
    public static boolean allocationMeasurementSupported() {
        return totalAllocatedBytes() >= 0;
    }

    /** Formats a byte count for a report line. */
    public static String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        if (bytes < 1024 * 1024) {
            return String.format(Locale.ROOT, "%.1f KiB", bytes / 1024.0);
        }
        if (bytes < 1024L * 1024 * 1024) {
            return String.format(Locale.ROOT, "%.1f MiB", bytes / (1024.0 * 1024));
        }
        return String.format(Locale.ROOT, "%.2f GiB", bytes / (1024.0 * 1024 * 1024));
    }
}
