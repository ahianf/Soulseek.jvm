// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;

/** Stops receiving public chat messages. */
public final class StopPublicChatCommand extends EmptyServerMessage {
    public StopPublicChatCommand() {
        super(MessageCode.Server.STOP_PUBLIC_CHAT);
    }
}
