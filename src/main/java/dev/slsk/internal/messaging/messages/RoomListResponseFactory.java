// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.room.RoomInfoMessage;
import dev.slsk.internal.room.RoomListMessage;
import java.util.ArrayList;
import java.util.List;

/** Parses the server's public and private chat-room lists. */
public final class RoomListResponseFactory implements IncomingMessage {
    private RoomListResponseFactory() {}

    /** Parses a room list. */
    public static RoomListMessage fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.ROOM_LIST, "RoomListResponseFactory", false);
        List<RoomInfoMessage> publicRooms = readRoomInfoList(reader);
        List<RoomInfoMessage> ownedRooms = readRoomInfoList(reader);
        List<RoomInfoMessage> privateRooms = readRoomInfoList(reader);
        List<String> moderatedRooms = readRoomNameList(reader);
        return new RoomListMessage(publicRooms, privateRooms, ownedRooms, moderatedRooms);
    }

    private static List<RoomInfoMessage> readRoomInfoList(MessageReader<MessageCode.Server> reader) {
        List<String> names = readRoomNameList(reader);
        int userCountCount = reader.readInteger();
        List<RoomInfoMessage> rooms = new ArrayList<>();
        for (int index = 0; index < userCountCount; index++) {
            rooms.add(new RoomInfoMessage(names.get(index), reader.readInteger()));
        }
        return rooms;
    }

    private static List<String> readRoomNameList(MessageReader<MessageCode.Server> reader) {
        int roomCount = reader.readInteger();
        List<String> names = new ArrayList<>();
        for (int index = 0; index < roomCount; index++) {
            names.add(reader.readString());
        }
        return names;
    }
}
