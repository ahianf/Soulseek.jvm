// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

/**
 * Event arguments raised when a chat-room message is received.
 */
public class RoomMessageReceivedEventArgs extends RoomEventArgs {
    private final String message;

    /**
     * Creates room-message event arguments.
     *
     * @param roomName the room in which the message was sent
     * @param username the user who sent the message
     * @param message the message content
     */
    public RoomMessageReceivedEventArgs(String roomName, String username, String message) {
        super(roomName, username);
        this.message = message;
    }

    /**
     * Returns the message content.
     *
     * @return the message
     */
    public final String getMessage() {
        return message;
    }
}
