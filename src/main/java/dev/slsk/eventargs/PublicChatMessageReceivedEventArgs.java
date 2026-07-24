// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

/**
 * Event arguments raised when a public chat message is received.
 */
public class PublicChatMessageReceivedEventArgs extends SoulseekClientEventArgs {
    private final String message;
    private final String roomName;
    private final String username;

    /**
     * Creates public-chat event arguments.
     *
     * @param roomName the room in which the message was sent
     * @param username the user who sent the message
     * @param message the message content
     */
    public PublicChatMessageReceivedEventArgs(String roomName, String username, String message) {
        this.roomName = roomName;
        this.username = username;
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

    /**
     * Returns the room in which the message was sent.
     *
     * @return the room name
     */
    public final String getRoomName() {
        return roomName;
    }

    /**
     * Returns the user who sent the message.
     *
     * @return the username
     */
    public final String getUsername() {
        return username;
    }
}
