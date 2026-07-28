// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;

/** The response to a request for a user's privilege status. */
public final class UserPrivilegeResponse implements IncomingMessage {
    private final boolean privileged;
    private final String username;

    /** Creates a privilege response. */
    public UserPrivilegeResponse(String username, boolean isPrivileged) {
        this.username = username;
        this.privileged = isPrivileged;
    }

    public boolean isPrivileged() {
        return privileged;
    }

    public String getUsername() {
        return username;
    }

    /** Parses a privilege response. */
    public static UserPrivilegeResponse fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.USER_PRIVILEGES, "UserPrivilegeResponse");
        return new UserPrivilegeResponse(reader.readString(), reader.readByte() > 0);
    }
}
