// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.user.Username;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

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
     */
    void addMember(String room, Username user) throws InterruptedException;

    void addMember(String room, Username user, Duration timeout) throws InterruptedException, TimeoutException;

    /**
     * Removes a member from a room we own.
     *
     * @param room the room
     * @param user who to remove
     */
    void removeMember(String room, Username user) throws InterruptedException;

    void removeMember(String room, Username user, Duration timeout) throws InterruptedException, TimeoutException;

    /**
     * Makes a member a moderator of a room we own.
     *
     * @param room the room
     * @param user who to promote
     */
    void addOperator(String room, Username user) throws InterruptedException;

    void addOperator(String room, Username user, Duration timeout) throws InterruptedException, TimeoutException;

    /**
     * Removes a moderator from a room we own.
     *
     * @param room the room
     * @param user who to demote
     */
    void removeOperator(String room, Username user) throws InterruptedException;

    void removeOperator(String room, Username user, Duration timeout) throws InterruptedException, TimeoutException;

    /**
     * Gives up our membership of a private room.
     *
     * @param room the room
     */
    void dropMembership(String room) throws InterruptedException;

    void dropMembership(String room, Duration timeout) throws InterruptedException, TimeoutException;

    /**
     * Gives up ownership of a private room.
     *
     * @param room the room
     */
    void dropOwnership(String room) throws InterruptedException;

    void dropOwnership(String room, Duration timeout) throws InterruptedException, TimeoutException;
}
