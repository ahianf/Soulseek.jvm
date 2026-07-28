// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

/** Progress data for a message payload being received. */
public final class MessageDataEvent extends MessageConnectionEvent {
    private final byte[] code;
    private final long currentLength;
    private final double percentComplete;
    private final long totalLength;

    /** Creates message payload progress data. */
    public MessageDataEvent(byte[] code, long currentLength, long totalLength) {
        this.code = code;
        this.currentLength = currentLength;
        this.totalLength = totalLength;
        percentComplete = (currentLength / (double) totalLength) * 100.0;
    }

    public byte[] getCode() {
        return code;
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
