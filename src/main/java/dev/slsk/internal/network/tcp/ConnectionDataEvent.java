// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

/** Progress data for a TCP read or write operation. */
public record ConnectionDataEvent(Connection connection, long currentLength, long totalLength)
        implements ConnectionEvent {

    /** Creates connection data progress. */
    public ConnectionDataEvent(long currentLength, long totalLength) {
        this(null, currentLength, totalLength);
    }

    public double percentComplete() {
        return (currentLength / (double) totalLength) * 100.0;
    }
}
