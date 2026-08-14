// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.user.UserData;
import dev.slsk.internal.user.UserPresence;

/** Notification that a user joined a chat room. */
public final class UserJoinedRoomNotification implements IncomingMessage {

    private final String roomName;
    private final UserData userData;
    private final String username;

    /** Creates a user-joined notification. */
    public UserJoinedRoomNotification(String roomName, String username, UserData userData) {
        this.roomName = roomName;
        this.username = username;
        this.userData = userData;
    }

    public String getRoomName() {
        return roomName;
    }

    public UserData getUserData() {
        return userData;
    }

    public String getUsername() {
        return username;
    }

    /** Parses a user-joined notification. */
    public static UserJoinedRoomNotification fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.USER_JOINED_ROOM, "UserJoinedRoomNotification");
        String roomName = reader.readString();
        String username = reader.readString();
        UserPresence presence = UserPresence.fromValue(reader.readInteger());
        int averageSpeed = reader.readInteger();
        long uploadCount = reader.readLong();
        int fileCount = reader.readInteger();
        int directoryCount = reader.readInteger();
        int slotsFree = reader.readInteger();
        String countryCode = reader.readString();
        UserData userData = new UserData(
                username, presence, averageSpeed, uploadCount, fileCount, directoryCount, countryCode, slotsFree);
        return new UserJoinedRoomNotification(roomName, username, userData);
    }
}
