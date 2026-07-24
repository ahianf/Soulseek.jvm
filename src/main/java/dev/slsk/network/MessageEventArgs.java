// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

/** Data for a complete message read or write. */
public final class MessageEventArgs extends MessageConnectionEventArgs {
    private final byte[] message;

    /** Creates complete-message event data. */
    public MessageEventArgs(byte[] message) {
        this.message = message;
    }

    public byte[] getMessage() {
        return message;
    }
}
