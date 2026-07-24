// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

/** Progress data for a TCP read or write operation. */
public final class ConnectionDataEvent extends ConnectionEvent {
    private final long currentLength;
    private final double percentComplete;
    private final long totalLength;

    /** Creates connection data progress. */
    public ConnectionDataEvent(long currentLength, long totalLength) {
        this.currentLength = currentLength;
        this.totalLength = totalLength;
        this.percentComplete = (currentLength / (double) totalLength) * 100.0;
    }

    public long getCurrentLength() {
        return currentLength;
    }

    public double getPercentComplete() {
        return percentComplete;
    }

    public long getTotalLength() {
        return totalLength;
    }
}
