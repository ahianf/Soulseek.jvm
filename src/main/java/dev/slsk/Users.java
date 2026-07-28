// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.events.UserEvent;
import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Other users: what they share, whether they are around, and where to reach
 * them.
 *
 * <p>Reading about a user is a question with an answer, so these block and
 * return values. A user who is offline is {@link UserPresence#OFFLINE}, not an
 * exception; exceptions here mean a fault, such as not being connected.
 *
 * <p>Nothing that changes <em>our</em> state lives here even when it names a
 * user. Banning is on {@code uploads()} because it changes our upload policy,
 * and gifting privileges is on {@code me()} because it spends our balance.
 */
public interface Users {

    /**
     * Asks a user to describe themselves.
     *
     * @param user who to ask
     * @param signal cancels the request
     * @return their self-description
     */
    UserInfo info(Username user, CancellationSignal signal);

    /**
     * Asks the server for a user's sharing figures.
     *
     * @param user who
     * @param signal cancels the request
     * @return their figures
     */
    UserStatistics statistics(Username user, CancellationSignal signal);

    /**
     * Asks the server whether a user is around.
     *
     * @param user who
     * @param signal cancels the request
     * @return their status; offline is an answer, not a failure
     */
    UserStatus status(Username user, CancellationSignal signal);

    /**
     * Resolves the address to connect to a user on.
     *
     * <p>The library caches these, so this is usually free. The cache is the
     * reason a consumer does not need one of its own.
     *
     * @param user who
     * @param signal cancels the lookup
     * @return where they are
     */
    InetSocketAddress endpoint(Username user, CancellationSignal signal);

    /**
     * Opens a status subscription, re-registered automatically on every login
     * and reference-counted across callers.
     *
     * @param user who to watch
     * @return the watch; close it to release
     */
    Watch watch(Username user);

    /**
     * Returns the users currently watched.
     *
     * @return the watched users
     */
    Set<Username> watched();

    /**
     * Returns the stream of user events.
     *
     * @return the event stream
     */
    EventStream<UserEvent> events();
}
