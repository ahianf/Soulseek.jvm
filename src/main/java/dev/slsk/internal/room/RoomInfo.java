// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.room;

import java.util.List;

/** Information about a chat room. */
public record RoomInfo(String name, int userCount, List<String> users) {

    public RoomInfo {
        users = users == null ? List.of() : List.copyOf(users);
    }

    public RoomInfo(String name, int userCount) {
        this(name, userCount, List.of());
    }

    public RoomInfo(String name, List<String> users) {
        this(name, users == null ? 0 : users.size(), users);
    }
}
