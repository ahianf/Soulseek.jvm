// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

/** Data for a complete message read or write. */
public final class MessageEvent extends MessageConnectionEvent {
    private final byte[] message;

    /** Creates complete-message event data. */
    public MessageEvent(byte[] message) {
        this.message = message;
    }

    public byte[] getMessage() {
        return message;
    }
}
