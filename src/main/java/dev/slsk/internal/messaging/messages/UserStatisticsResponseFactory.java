// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.UserStatistics;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;

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
