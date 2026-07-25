// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.soak;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

/**
 * Measures process CPU time.
 *
 * <p>The polling defects (1.4) are invisible to throughput measurement — a
 * semaphore acquired by spinning at 25 ms still eventually acquires. What they
 * cost is CPU burned while nothing is happening, which is what this measures.
 */
public final class CpuProbe {

    private static final OperatingSystemMXBean OS = ManagementFactory.getOperatingSystemMXBean();

    private CpuProbe() {}

    /** Returns cumulative process CPU time in nanoseconds, or -1 when unsupported. */
    public static long processCpuNanos() {
        if (OS instanceof com.sun.management.OperatingSystemMXBean sunOs) {
            return sunOs.getProcessCpuTime();
        }
        return -1;
    }

    /** Returns whether CPU measurement is available on this JVM. */
    public static boolean supported() {
        return processCpuNanos() >= 0;
    }

    /**
     * Runs a body and returns the CPU time it consumed, as a fraction of one
     * core over the wall-clock duration.
     *
     * <p>A value near zero means the work was genuinely blocked. A value near
     * one means one core spun for the whole interval.
     */
    public static double coreFractionDuring(Runnable body) {
        long cpuStart = processCpuNanos();
        long wallStart = System.nanoTime();
        body.run();
        long wallElapsed = System.nanoTime() - wallStart;
        long cpuElapsed = processCpuNanos() - cpuStart;
        if (cpuStart < 0 || wallElapsed <= 0) {
            return -1;
        }
        return (double) cpuElapsed / wallElapsed;
    }
}
