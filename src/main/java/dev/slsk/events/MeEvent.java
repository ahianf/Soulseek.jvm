// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import dev.slsk.UserPresence;
import dev.slsk.Username;
import dev.slsk.connection.ServerInfo;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Things that happened to this account, as opposed to the connection carrying it.
 */
public sealed interface MeEvent extends SoulseekEvent {

    /**
     * The server accepted our credentials.
     *
     * @param server what the server said about itself
     * @param at when
     */
    record LoggedIn(ServerInfo server, Instant at) implements MeEvent {
        public LoggedIn {
            Objects.requireNonNull(server, "server");
            Objects.requireNonNull(at, "at");
        }
    }

    /**
     * Somebody gave us privileges.
     *
     * <p>Acknowledged automatically, on the same rule as a private message.
     *
     * @param from who gave them, if the server said
     * @param at when
     */
    record PrivilegeNotificationReceived(Username from, Instant at) implements MeEvent {
        public PrivilegeNotificationReceived {
            Objects.requireNonNull(at, "at");
        }
    }

    /**
     * The server's list of privileged users.
     *
     * @param users the privileged users
     * @param at when
     */
    record PrivilegedUserListReceived(List<Username> users, Instant at) implements MeEvent {
        public PrivilegedUserListReceived {
            users = List.copyOf(Objects.requireNonNull(users, "users"));
            Objects.requireNonNull(at, "at");
        }
    }

    /**
     * We changed our own presence.
     *
     * @param from the previous presence
     * @param to the new presence
     * @param at when
     */
    record PresenceChanged(UserPresence from, UserPresence to, Instant at) implements MeEvent {
        public PresenceChanged {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(to, "to");
            Objects.requireNonNull(at, "at");
        }
    }
}
