// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.user.Username;

/**
 * Administration of private rooms we own or moderate.
 *
 * <p>Split off {@link Rooms} because it is a different job done by a different
 * person: joining and talking is what everyone does, and adding members and
 * handing out moderator status is what an owner does. Keeping them apart holds
 * {@code Rooms} to the size where its members can be found by reading them.
 *
 * <p>Every method here is an idempotent intent. Adding a member who is already a
 * member does nothing rather than failing.
 */
public interface PrivateRooms {

    /**
     * Adds a member to a room we own.
     *
     * @param room the room
     * @param user who to add
     * @param signal cancels the request
     */
    void addMember(String room, Username user, CancellationSignal signal);

    /**
     * Removes a member from a room we own.
     *
     * @param room the room
     * @param user who to remove
     * @param signal cancels the request
     */
    void removeMember(String room, Username user, CancellationSignal signal);

    /**
     * Makes a member a moderator of a room we own.
     *
     * @param room the room
     * @param user who to promote
     * @param signal cancels the request
     */
    void addOperator(String room, Username user, CancellationSignal signal);

    /**
     * Removes a moderator from a room we own.
     *
     * @param room the room
     * @param user who to demote
     * @param signal cancels the request
     */
    void removeOperator(String room, Username user, CancellationSignal signal);

    /**
     * Gives up our membership of a private room.
     *
     * @param room the room
     * @param signal cancels the request
     */
    void dropMembership(String room, CancellationSignal signal);

    /**
     * Gives up ownership of a private room.
     *
     * @param room the room
     * @param signal cancels the request
     */
    void dropOwnership(String room, CancellationSignal signal);
}
