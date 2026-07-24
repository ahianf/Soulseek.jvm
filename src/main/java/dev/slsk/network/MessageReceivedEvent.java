// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

/** Metadata raised after a message header is received. */
public final class MessageReceivedEvent extends MessageConnectionEvent {
    private final byte[] code;
    private final long length;

    /** Creates received-message header data. */
    public MessageReceivedEvent(long length, byte[] code) {
        this.length = length;
        this.code = code;
    }

    public byte[] getCode() {
        return code;
    }

    public long getLength() {
        return length;
    }
}
