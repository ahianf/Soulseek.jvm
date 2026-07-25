// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.soak;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Locale;

/**
 * Collects scenario measurements into {@code target/soak-report.txt}.
 *
 * <p>The numbers this writes are what gets transcribed into
 * {@code docs/fork-divergence.md}. Phase 0 records a baseline against the
 * unmodified 0.11.0 tree; every later phase re-runs and compares.
 */
public final class SoakReport {

    private static final Path REPORT = Paths.get("target", "soak-report.txt");
    private static final Object LOCK = new Object();

    private SoakReport() {}

    /** Records a named measurement with a unit. */
    public static void record(String scenario, String metric, Object value) {
        write(String.format(Locale.ROOT, "%-34s %-38s %s", scenario, metric, value));
    }

    /** Records a free-form note against a scenario. */
    public static void note(String scenario, String text) {
        write(String.format(Locale.ROOT, "%-34s %s", scenario, text));
    }

    private static void write(String line) {
        System.out.println("[soak] " + line);
        synchronized (LOCK) {
            try {
                Path parent = REPORT.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                Files.writeString(
                        REPORT,
                        line + System.lineSeparator(),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
            } catch (IOException exception) {
                // A report-write failure must not mask a scenario result.
                System.err.println("[soak] failed to write report line: " + exception.getMessage());
            }
        }
    }
}
