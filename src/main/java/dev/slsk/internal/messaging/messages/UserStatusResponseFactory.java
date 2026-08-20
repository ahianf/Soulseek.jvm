// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.user.UserStatusSnapshot;
import dev.slsk.internal.user.WireUserPresence;

/** Parses responses to user-status requests. */
public final class UserStatusResponseFactory {
    private UserStatusResponseFactory() {}

    /** Parses a user status. */
    public static UserStatusSnapshot fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.GET_STATUS, "UserStatusResponseFactory");
        return new UserStatusSnapshot(
                reader.readString(), WireUserPresence.fromValue(reader.readInteger()), reader.readByte() > 0);
    }
}
