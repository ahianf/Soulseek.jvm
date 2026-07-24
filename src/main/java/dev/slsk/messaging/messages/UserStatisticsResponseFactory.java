// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.UserStatistics;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** Parses responses to user-statistics requests. */
public final class UserStatisticsResponseFactory {
    private UserStatisticsResponseFactory() {}

    /** Parses user statistics. */
    public static UserStatistics fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.GET_USER_STATS, "UserStatistics");
        return new UserStatistics(
                reader.readString(),
                reader.readInteger(),
                reader.readLong(),
                reader.readInteger(),
                reader.readInteger());
    }
}
