// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.room;

import java.util.List;

/** The server's public and private chat-room lists. */
public record RoomList(
        List<RoomInfo> publicRooms,
        List<RoomInfo> privateRooms,
        List<RoomInfo> ownedRooms,
        List<String> moderatedRoomNames) {

    public RoomList {
        publicRooms = publicRooms == null ? List.of() : List.copyOf(publicRooms);
        privateRooms = privateRooms == null ? List.of() : List.copyOf(privateRooms);
        ownedRooms = ownedRooms == null ? List.of() : List.copyOf(ownedRooms);
        moderatedRoomNames = moderatedRoomNames == null ? List.of() : List.copyOf(moderatedRoomNames);
    }

    public int publicCount() {
        return publicRooms.size();
    }

    public int privateCount() {
        return privateRooms.size();
    }

    public int ownedCount() {
        return ownedRooms.size();
    }

    public int moderatedRoomNameCount() {
        return moderatedRoomNames.size();
    }
}
