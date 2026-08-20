// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

/** Metadata emitted after a message header is received. */
public record MessageReceivedEvent(MessageConnection connection, long length, byte[] code)
        implements MessageConnectionEvent {

    /** Creates received-message header data. */
    public MessageReceivedEvent(long length, byte[] code) {
        this(null, length, code);
    }
}
