// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.room;

import dev.slsk.internal.user.UserData;
import java.util.List;

/** The response to a request to join a chat room. */
public record RoomData(String name, List<UserData> users, boolean privateRoom, String owner, List<String> operators) {

    public RoomData {
        users = users == null ? List.of() : List.copyOf(users);
        operators = operators == null ? null : List.copyOf(operators);
    }

    public RoomData(String name, List<UserData> users) {
        this(name, users, false, null, null);
    }

    public RoomData(String name, List<UserData> users, boolean privateRoom) {
        this(name, users, privateRoom, null, null);
    }

    public RoomData(String name, List<UserData> users, boolean privateRoom, String owner) {
        this(name, users, privateRoom, owner, null);
    }

    public int userCount() {
        return users.size();
    }

    public Integer operatorCount() {
        return operators == null ? null : operators.size();
    }
}
