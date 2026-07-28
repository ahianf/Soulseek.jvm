// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;

/** Starts receiving public chat messages. */
public final class StartPublicChatCommand extends EmptyServerMessage {
    public StartPublicChatCommand() {
        super(MessageCode.Server.ASK_PUBLIC_CHAT);
    }
}
