// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.room;

import java.util.List;
import java.util.Objects;

/**
 * The server's directory of rooms, split by how we relate to each one.
 *
 * @param publicRooms rooms anyone may join
 * @param privateRooms private rooms we are a member of
 * @param owned private rooms we own
 * @param moderated private rooms we moderate
 */
public record RoomList(
        List<RoomInfo> publicRooms, List<RoomInfo> privateRooms, List<RoomInfo> owned, List<String> moderated) {

    /** Validates and returns the list. */
    public RoomList {
        publicRooms = List.copyOf(Objects.requireNonNull(publicRooms, "publicRooms"));
        privateRooms = List.copyOf(Objects.requireNonNull(privateRooms, "privateRooms"));
        owned = List.copyOf(Objects.requireNonNull(owned, "owned"));
        moderated = List.copyOf(Objects.requireNonNull(moderated, "moderated"));
    }

    /** Returns an empty list. */
    public static RoomList empty() {
        return new RoomList(List.of(), List.of(), List.of(), List.of());
    }
}
