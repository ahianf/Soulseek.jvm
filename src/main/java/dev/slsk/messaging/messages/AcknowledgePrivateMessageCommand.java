// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;

/** Acknowledges receipt of a private message. */
public final class AcknowledgePrivateMessageCommand extends IntegerServerMessage {
    public AcknowledgePrivateMessageCommand(int id) {
        super(MessageCode.Server.ACKNOWLEDGE_PRIVATE_MESSAGE, id);
    }

    public int getId() {
        return value();
    }
}
