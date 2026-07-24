// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Information about a chat room.
 */
public class RoomInfo {
    private final String name;
    private final int userCount;
    private final List<String> users;

    /**
     * Creates room information when only the server-reported user count is
     * available.
     *
     * @param name the room name
     * @param userCount the number of users in the room
     */
    public RoomInfo(String name, int userCount) {
        this.name = name;
        this.users = Collections.emptyList();
        this.userCount = userCount;
    }

    /**
     * Creates room information with its users.
     *
     * @param name the room name
     * @param userList the users in the room, if available
     */
    public RoomInfo(String name, Iterable<String> userList) {
        this.name = name;

        List<String> copiedUsers = new ArrayList<>();
        if (userList != null) {
            userList.forEach(copiedUsers::add);
        }
        users = Collections.unmodifiableList(copiedUsers);
        userCount = users.size();
    }

    /**
     * Returns the room name.
     *
     * @return the room name
     */
    public final String getName() {
        return name;
    }

    /**
     * Returns the number of users in the room.
     *
     * @return the user count
     */
    public final int getUserCount() {
        return userCount;
    }

    /**
     * Returns the users as an immutable snapshot.
     *
     * @return the users
     */
    public final List<String> getUsers() {
        return users;
    }
}
