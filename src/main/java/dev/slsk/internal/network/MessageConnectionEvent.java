// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

/** Base type for message-connection event payloads. */
public sealed interface MessageConnectionEvent permits MessageDataEvent, MessageEvent, MessageReceivedEvent {
    /** Returns the connection that emitted this event, when known. */
    MessageConnection connection();
}
