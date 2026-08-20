// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

/** Progress data for a message payload being received. */
public record MessageDataEvent(MessageConnection connection, byte[] code, long currentLength, long totalLength)
        implements MessageConnectionEvent {

    /** Creates message payload progress data. */
    public MessageDataEvent(byte[] code, long currentLength, long totalLength) {
        this(null, code, currentLength, totalLength);
    }

    public double percentComplete() {
        return (currentLength / (double) totalLength) * 100.0;
    }
}
