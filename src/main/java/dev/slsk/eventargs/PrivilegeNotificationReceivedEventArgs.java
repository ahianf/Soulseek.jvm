// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

/**
 * Event arguments raised when new privileges are announced.
 */
public class PrivilegeNotificationReceivedEventArgs extends SoulseekClientEventArgs {
    private final Integer id;
    private final boolean requiresAcknowlegement;
    private final String username;

    /**
     * Creates a privilege notification without an acknowledgement identifier.
     *
     * @param username the new privileged user
     */
    public PrivilegeNotificationReceivedEventArgs(String username) {
        this(username, null);
    }

    /**
     * Creates a privilege notification.
     *
     * @param username the new privileged user
     * @param id the notification identifier, or {@code null} when absent
     */
    public PrivilegeNotificationReceivedEventArgs(String username, Integer id) {
        this.username = username;
        this.id = id;
        this.requiresAcknowlegement = id != null;
    }

    /**
     * Returns the notification identifier, if applicable.
     *
     * @return the identifier, or {@code null}
     */
    public final Integer getId() {
        return id;
    }

    /**
     * Returns whether the notification must be acknowledged.
     *
     * @return {@code true} when acknowledgement is required
     */
    public final boolean isRequiresAcknowlegement() {
        return requiresAcknowlegement;
    }

    /**
     * Returns the new privileged user.
     *
     * @return the username
     */
    public final String getUsername() {
        return username;
    }
}
