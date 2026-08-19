// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.events.UserEvent;
import dev.slsk.share.Directory;
import dev.slsk.user.Browse;
import dev.slsk.user.BrowseRequest;
import dev.slsk.user.UserInfo;
import dev.slsk.user.UserPresence;
import dev.slsk.user.UserStatistics;
import dev.slsk.user.UserStatus;
import dev.slsk.user.Username;
import dev.slsk.user.Watch;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

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
     * @return their self-description
     */
    UserInfo info(Username user) throws InterruptedException;

    UserInfo info(Username user, Duration timeout) throws InterruptedException, TimeoutException;

    /**
     * Asks the server for a user's sharing figures.
     *
     * @param user who
     * @return their figures
     */
    UserStatistics statistics(Username user) throws InterruptedException;

    UserStatistics statistics(Username user, Duration timeout) throws InterruptedException, TimeoutException;

    /**
     * Asks the server whether a user is around.
     *
     * @param user who
     * @return their status; offline is an answer, not a failure
     */
    UserStatus status(Username user) throws InterruptedException;

    UserStatus status(Username user, Duration timeout) throws InterruptedException, TimeoutException;

    /**
     * Resolves the address to connect to a user on.
     *
     * <p>The library caches these, so this is usually free. The cache is the
     * reason a consumer does not need one of its own.
     *
     * @param user who
     * @return where they are
     */
    InetSocketAddress endpoint(Username user) throws InterruptedException;

    InetSocketAddress endpoint(Username user, Duration timeout) throws InterruptedException, TimeoutException;

    /**
     * Reads everything a user is sharing.
     *
     * @param request whose share to read, and how patiently
     * @return what they were sharing, at the moment they answered
     */
    Browse browse(BrowseRequest request) throws InterruptedException;

    Browse browse(BrowseRequest request, Duration timeout) throws InterruptedException, TimeoutException;

    /**
     * Reads the contents of one of a user's directories.
     *
     * <p>A list rather than a single directory, because the protocol answers a
     * folder request with one and a peer is free to include subdirectories.
     *
     * @param user whose share to read
     * @param path the directory's full remote path
     * @return the directories they answered with, empty if none
     */
    List<Directory> directory(Username user, String path) throws InterruptedException;

    List<Directory> directory(Username user, String path, Duration timeout)
            throws InterruptedException, TimeoutException;

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
