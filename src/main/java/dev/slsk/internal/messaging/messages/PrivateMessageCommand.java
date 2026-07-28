// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;

/** Sends a private message. */
public final class PrivateMessageCommand implements OutgoingMessage {
    private final String message;
    private final String username;

    public PrivateMessageCommand(String username, String message) {
        this.username = username;
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public String getUsername() {
        return username;
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.PRIVATE_MESSAGE)
                .writeString(username)
                .writeString(message)
                .build();
    }
}
