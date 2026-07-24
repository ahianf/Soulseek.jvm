// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The server's public and private chat-room lists.
 */
public class RoomList {
    private final int moderatedRoomNameCount;
    private final List<String> moderatedRoomNames;
    private final int ownedCount;
    private final List<RoomInfo> ownedRooms;
    private final int privateCount;
    private final List<RoomInfo> privateRooms;
    private final int publicCount;
    private final List<RoomInfo> publicRooms;

    /**
     * Creates a room list.
     *
     * @param publicList the public rooms
     * @param privateList the private rooms
     * @param ownedList the rooms owned by the logged-in user
     * @param moderatedRoomNameList the rooms moderated by the logged-in user
     */
    public RoomList(
            Iterable<? extends RoomInfo> publicList,
            Iterable<? extends RoomInfo> privateList,
            Iterable<? extends RoomInfo> ownedList,
            Iterable<String> moderatedRoomNameList) {
        publicRooms = immutableCopy(publicList);
        publicCount = publicRooms.size();
        privateRooms = immutableCopy(privateList);
        privateCount = privateRooms.size();
        ownedRooms = immutableCopy(ownedList);
        ownedCount = ownedRooms.size();
        moderatedRoomNames = immutableStringCopy(moderatedRoomNameList);
        moderatedRoomNameCount = moderatedRoomNames.size();
    }

    /**
     * Returns the number of public rooms.
     *
     * @return the public-room count
     */
    public final int getPublicCount() {
        return publicCount;
    }

    /**
     * Returns the number of private rooms.
     *
     * @return the private-room count
     */
    public final int getPrivateCount() {
        return privateCount;
    }

    /**
     * Returns the number of owned rooms.
     *
     * @return the owned-room count
     */
    public final int getOwnedCount() {
        return ownedCount;
    }

    /**
     * Returns the number of moderated room names.
     *
     * @return the moderated-room-name count
     */
    public final int getModeratedRoomNameCount() {
        return moderatedRoomNameCount;
    }

    /**
     * Returns the public rooms as an immutable snapshot.
     *
     * @return the public rooms
     */
    public final List<RoomInfo> getPublic() {
        return publicRooms;
    }

    /**
     * Returns the private rooms as an immutable snapshot.
     *
     * @return the private rooms
     */
    public final List<RoomInfo> getPrivate() {
        return privateRooms;
    }

    /**
     * Returns the owned rooms as an immutable snapshot.
     *
     * @return the owned rooms
     */
    public final List<RoomInfo> getOwned() {
        return ownedRooms;
    }

    /**
     * Returns the moderated room names as an immutable snapshot.
     *
     * @return the moderated room names
     */
    public final List<String> getModeratedRoomNames() {
        return moderatedRoomNames;
    }

    private static List<RoomInfo> immutableCopy(Iterable<? extends RoomInfo> source) {
        List<RoomInfo> copy = new ArrayList<>();
        if (source != null) {
            source.forEach(copy::add);
        }
        return Collections.unmodifiableList(copy);
    }

    private static List<String> immutableStringCopy(Iterable<String> source) {
        List<String> copy = new ArrayList<>();
        if (source != null) {
            source.forEach(copy::add);
        }
        return Collections.unmodifiableList(copy);
    }
}
