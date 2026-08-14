// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.RoomData;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.user.UserData;
import dev.slsk.internal.user.UserPresence;
import java.util.ArrayList;
import java.util.List;

/** Parses responses to requests to join chat rooms. */
public final class JoinRoomResponse implements IncomingMessage {
    private JoinRoomResponse() {}

    /** Parses joined-room data. */
    public static RoomData fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.JOIN_ROOM, "JoinRoomResponse", false);
        String roomName = reader.readString();

        int userCount = reader.readInteger();
        List<String> usernames = new ArrayList<>();
        for (int index = 0; index < userCount; index++) {
            usernames.add(reader.readString());
        }

        int statusCount = reader.readInteger();
        List<UserPresence> statuses = new ArrayList<>();
        for (int index = 0; index < statusCount; index++) {
            statuses.add(UserPresence.fromValue(reader.readInteger()));
        }

        int dataCount = reader.readInteger();
        List<UserNumbers> numbers = new ArrayList<>();
        for (int index = 0; index < dataCount; index++) {
            numbers.add(new UserNumbers(
                    reader.readInteger(), reader.readLong(), reader.readInteger(), reader.readInteger()));
        }

        int slotsFreeCount = reader.readInteger();
        List<Integer> slots = new ArrayList<>();
        for (int index = 0; index < slotsFreeCount; index++) {
            slots.add(reader.readInteger());
        }

        int countryCount = reader.readInteger();
        List<String> countries = new ArrayList<>();
        for (int index = 0; index < countryCount; index++) {
            countries.add(reader.readString());
        }

        List<UserData> users = new ArrayList<>();
        for (int index = 0; index < userCount; index++) {
            UserNumbers data = numbers.get(index);
            users.add(new UserData(
                    usernames.get(index),
                    statuses.get(index),
                    data.averageSpeed(),
                    data.uploadCount(),
                    data.fileCount(),
                    data.directoryCount(),
                    countries.get(index),
                    slots.get(index)));
        }

        if (!reader.hasMoreData()) {
            return new RoomData(roomName, users);
        }

        String owner = reader.readString();
        int operatorCount = reader.readInteger();
        List<String> operators = new ArrayList<>();
        for (int index = 0; index < operatorCount; index++) {
            operators.add(reader.readString());
        }
        return new RoomData(roomName, users, owner != null, owner, operators);
    }

    private record UserNumbers(int averageSpeed, long uploadCount, int fileCount, int directoryCount) {}
}
