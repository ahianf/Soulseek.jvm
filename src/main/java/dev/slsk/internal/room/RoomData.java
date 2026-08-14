// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.room;

import dev.slsk.internal.user.UserData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The response to a request to join a chat room.
 */
public class RoomData {
    private final boolean privateRoom;
    private final String name;
    private final Integer operatorCount;
    private final List<String> operators;
    private final String owner;
    private final int userCount;
    private final List<UserData> users;

    /**
     * Creates public room data.
     *
     * @param name the joined room name
     * @param userList the users in the room
     */
    public RoomData(String name, Iterable<? extends UserData> userList) {
        this(name, userList, false, null, null);
    }

    /**
     * Creates room data.
     *
     * @param name the joined room name
     * @param userList the users in the room
     * @param isPrivate whether the room is private
     */
    public RoomData(String name, Iterable<? extends UserData> userList, boolean isPrivate) {
        this(name, userList, isPrivate, null, null);
    }

    /**
     * Creates room data.
     *
     * @param name the joined room name
     * @param userList the users in the room
     * @param isPrivate whether the room is private
     * @param owner the private-room owner
     */
    public RoomData(String name, Iterable<? extends UserData> userList, boolean isPrivate, String owner) {
        this(name, userList, isPrivate, owner, null);
    }

    /**
     * Creates room data.
     *
     * @param name the joined room name
     * @param userList the users in the room
     * @param isPrivate whether the room is private
     * @param owner the private-room owner
     * @param operatorList the private-room operators
     */
    public RoomData(
            String name,
            Iterable<? extends UserData> userList,
            boolean isPrivate,
            String owner,
            Iterable<String> operatorList) {
        this.name = name;
        users = immutableUserCopy(userList);
        userCount = users.size();
        privateRoom = isPrivate;
        this.owner = owner;
        operators = operatorList == null ? null : immutableStringCopy(operatorList);
        operatorCount = operators == null ? null : operators.size();
    }

    /**
     * Returns whether the room is private.
     *
     * @return whether the room is private
     */
    public final boolean isPrivate() {
        return privateRoom;
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
     * Returns the operator count, if an operator list was supplied.
     *
     * @return the operator count, or {@code null}
     */
    public final Integer getOperatorCount() {
        return operatorCount;
    }

    /**
     * Returns the operators as an immutable snapshot, if supplied.
     *
     * @return the operators, or {@code null}
     */
    public final List<String> getOperators() {
        return operators;
    }

    /**
     * Returns the private-room owner.
     *
     * @return the owner, or {@code null}
     */
    public final String getOwner() {
        return owner;
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
    public final List<UserData> getUsers() {
        return users;
    }

    private static List<UserData> immutableUserCopy(Iterable<? extends UserData> source) {
        List<UserData> copy = new ArrayList<>();
        if (source != null) {
            source.forEach(copy::add);
        }
        return Collections.unmodifiableList(copy);
    }

    private static List<String> immutableStringCopy(Iterable<String> source) {
        List<String> copy = new ArrayList<>();
        source.forEach(copy::add);
        return Collections.unmodifiableList(copy);
    }
}
