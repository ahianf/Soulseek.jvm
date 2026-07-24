// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.UserData;
import dev.slsk.UserPresence;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** The response to a request to watch a user. */
public final class WatchUserResponse implements IncomingMessage {
    private final boolean exists;
    private final UserData userData;
    private final String username;

    /** Creates a response for a user without additional data. */
    public WatchUserResponse(String username, boolean exists) {
        this(username, exists, null);
    }

    /** Creates a watch-user response. */
    public WatchUserResponse(String username, boolean exists, UserData userData) {
        this.username = username;
        this.exists = exists;
        this.userData = userData;
    }

    public boolean isExists() {
        return exists;
    }

    public UserData getUserData() {
        return userData;
    }

    public String getUsername() {
        return username;
    }

    /** Parses a watch-user response. */
    public static WatchUserResponse fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.WATCH_USER, "WatchUserResponse");
        String username = reader.readString();
        boolean exists = reader.readByte() > 0;
        if (!exists) {
            return new WatchUserResponse(username, false);
        }

        UserPresence presence = UserPresence.fromValue(reader.readInteger());
        int averageSpeed = reader.readInteger();
        long uploadCount = reader.readLong();
        int fileCount = reader.readInteger();
        int directoryCount = reader.readInteger();
        String countryCode = reader.isHasMoreData() ? reader.readString() : null;
        return new WatchUserResponse(
                username,
                true,
                new UserData(username, presence, averageSpeed, uploadCount, fileCount, directoryCount, countryCode));
    }
}
