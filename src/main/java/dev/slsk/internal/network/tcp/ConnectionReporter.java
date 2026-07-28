// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

/** Reports requested, granted, and transferred byte counts. */
@FunctionalInterface
public interface ConnectionReporter {
    /** Reports one completed read or write iteration. */
    void report(int requestedBytes, int grantedBytes, int transferredBytes);
}
