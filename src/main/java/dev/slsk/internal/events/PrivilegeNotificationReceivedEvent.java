// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

/** Event payload emitted when new privileges are announced. */
public record PrivilegeNotificationReceivedEvent(String username, Integer id) implements SoulseekClientEvent {

    /**
     * Creates a privilege notification without an acknowledgement identifier.
     *
     * @param username the new privileged user
     */
    public PrivilegeNotificationReceivedEvent(String username) {
        this(username, null);
    }

    /** Returns whether the notification must be acknowledged. */
    public boolean requiresAcknowledgement() {
        return id != null;
    }
}
