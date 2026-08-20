// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

/** Data for a complete message read or write. */
public record MessageEvent(MessageConnection connection, byte[] message) implements MessageConnectionEvent {

    /** Creates complete-message event data. */
    public MessageEvent(byte[] message) {
        this(null, message);
    }
}
