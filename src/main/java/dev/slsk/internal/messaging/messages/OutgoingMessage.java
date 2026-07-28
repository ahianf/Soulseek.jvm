// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

/** A protocol message that can be serialized for a connection. */
public interface OutgoingMessage {
    /**
     * Serializes this message.
     *
     * @return the framed message bytes
     */
    byte[] toByteArray();
}
