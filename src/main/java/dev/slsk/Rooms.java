// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.events.RoomEvent;
import dev.slsk.room.Room;
import dev.slsk.room.RoomList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Chat rooms.
 *
 * <p>{@link #get(String)} and {@link #joined()} answer with state — who is in a
 * room, what is pinned there — and never with messages. What was said is
 * history, and history belongs to the application.
 *
 * <p>Private-room administration lives on {@link #privateRooms()}, because
 * owning a room is a different job from being in one.
 */
public interface Rooms {

    /**
     * Asks the server for its room directory.
     *
     * @param signal cancels the request
     * @return the directory
     */
    RoomList list(CancellationSignal signal);

    /**
     * Joins a room.
     *
     * <p>An idempotent intent: joining a room we are already in returns its
     * current state rather than failing.
     *
     * @param room the room to join
     * @param signal cancels the request
     * @return the room as it stands
     */
    Room join(String room, CancellationSignal signal);

    /**
     * Leaves a room. Leaving a room we are not in does nothing.
     *
     * @param room the room to leave
     */
    void leave(String room);

    /**
     * Says something in a room.
     *
     * @param room the room
     * @param message what to say
     */
    void say(String room, String message);

    /**
     * Pins our ticker in a room, replacing whatever we pinned before.
     *
     * @param room the room
     * @param message what to pin
     */
    void setTicker(String room, String message);

    /**
     * Returns a room we are in.
     *
     * @param room the room name
     * @return its current state
     * @throws IllegalArgumentException if we are not in it
     */
    Room get(String room);

    /**
     * Returns every room we are in.
     *
     * @return the rooms
     */
    List<Room> joined();

    /**
     * Starts the all-rooms message firehose. Idempotent.
     */
    void startPublicChat();

    /**
     * Stops the all-rooms message firehose. Idempotent.
     */
    void stopPublicChat();

    /**
     * Returns administration for private rooms.
     *
     * @return the private-room facet
     */
    PrivateRooms privateRooms();

    /**
     * Returns the stream of room events.
     *
     * @return the event stream
     */
    EventStream<RoomEvent> events();

    /**
     * Takes the joined rooms and subscribes, as one atomic step.
     *
     * @param listener receives every subsequent event
     * @return the rooms as they were, and the subscription
     */
    Attachment<List<Room>> attach(Consumer<RoomEvent> listener);
}
