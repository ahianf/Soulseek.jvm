// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import dev.slsk.UserStatistics;
import dev.slsk.UserStatus;
import dev.slsk.Username;
import java.time.Instant;
import java.util.Objects;

/**
 * What changed about users we are watching.
 *
 * <p>These arrive only for users under an active {@code watch}. Soulseek's
 * subscription is server-side and does not survive a reconnect, so the library
 * re-registers every watch on login; a consumer that has a {@code Watch} open
 * keeps receiving events across a disconnect without doing anything.
 */
public sealed interface UserEvent extends SoulseekEvent {

    /**
     * A watched user came online, went away, or went offline.
     *
     * @param user who
     * @param from their previous status
     * @param to their current status
     * @param at when
     */
    record StatusChanged(Username user, UserStatus from, UserStatus to, Instant at) implements UserEvent {
        public StatusChanged {
            Objects.requireNonNull(user, "user");
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(to, "to");
            Objects.requireNonNull(at, "at");
        }
    }

    /**
     * A watched user's sharing figures changed.
     *
     * @param user who
     * @param statistics their current figures
     * @param at when
     */
    record StatisticsChanged(Username user, UserStatistics statistics, Instant at) implements UserEvent {
        public StatisticsChanged {
            Objects.requireNonNull(user, "user");
            Objects.requireNonNull(statistics, "statistics");
            Objects.requireNonNull(at, "at");
        }
    }

    /**
     * We could not reach a user we tried to connect to.
     *
     * @param user who
     * @param reason what went wrong
     * @param at when
     */
    record CannotConnect(Username user, String reason, Instant at) implements UserEvent {
        public CannotConnect {
            Objects.requireNonNull(user, "user");
            Objects.requireNonNull(reason, "reason");
            Objects.requireNonNull(at, "at");
        }
    }
}
