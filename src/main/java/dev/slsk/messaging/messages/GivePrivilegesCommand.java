// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;

/** Grants privileges to a user. */
public final class GivePrivilegesCommand implements IOutgoingMessage {
    private final int days;
    private final String username;

    public GivePrivilegesCommand(String username, int days) {
        this.username = username;
        this.days = days;
    }

    public int getDays() {
        return days;
    }

    public String getUsername() {
        return username;
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.GIVE_PRIVILEGES)
                .writeString(username)
                .writeInteger(days)
                .build();
    }
}
