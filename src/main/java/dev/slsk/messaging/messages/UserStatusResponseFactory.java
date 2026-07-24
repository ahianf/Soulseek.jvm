// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.UserPresence;
import dev.slsk.UserStatus;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** Parses responses to user-status requests. */
public final class UserStatusResponseFactory {
    private UserStatusResponseFactory() {}

    /** Parses a user status. */
    public static UserStatus fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.GET_STATUS, "UserStatusResponseFactory");
        return new UserStatus(reader.readString(), UserPresence.fromValue(reader.readInteger()), reader.readByte() > 0);
    }
}
