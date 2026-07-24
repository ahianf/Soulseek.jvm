// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** An incoming notification of granted privileges. */
public final class PrivilegeNotification implements IIncomingMessage {
    private final int id;
    private final String username;

    /** Creates a privilege notification. */
    public PrivilegeNotification(int id, String username) {
        this.id = id;
        this.username = username;
    }

    public int getId() {
        return id;
    }

    public String getUsername() {
        return username;
    }

    /** Parses a privilege notification. */
    public static PrivilegeNotification fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.NOTIFY_PRIVILEGES, "PrivilegeNotification");
        return new PrivilegeNotification(reader.readInteger(), reader.readString());
    }
}
