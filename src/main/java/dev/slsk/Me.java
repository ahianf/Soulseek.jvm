// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.events.MeEvent;
import dev.slsk.user.UserPresence;
import dev.slsk.user.UserProfile;
import dev.slsk.user.Username;

/**
 * This account: who we are, and the things only we can change about ourselves.
 *
 * <p>{@code giftPrivileges} lives here rather than on {@code users()} because of
 * what it changes. It names another user, but what it spends is <em>our</em>
 * privilege balance, and the facet a method belongs to is the one owning the
 * state it mutates. The same test puts {@code ban} on {@code uploads()}, where
 * it changes our upload policy rather than anything about the user it names.
 */
public interface Me {

    /**
     * Returns the account we are logged in as.
     *
     * @return our username
     */
    Username username();

    /**
     * Returns the presence we last published.
     *
     * @return our presence
     */
    UserPresence presence();

    /**
     * Publishes our presence to the server.
     *
     * <p>An idempotent intent: setting the presence we already have does
     * nothing.
     *
     * @param presence what to publish
     */
    void presence(UserPresence presence);

    /**
     * Returns what other users see when they ask about this account.
     *
     * @return the profile
     */
    UserProfile profile();

    /**
     * Sets what other users see when they ask about this account.
     *
     * <p>Set once, served to every peer who asks. The callback this replaces
     * was invoked per request, which every application answered with the same
     * constant.
     *
     * @param profile the profile to serve
     */
    void profile(UserProfile profile);

    /**
     * Returns how many days of privileges remain on this account.
     *
     * @param signal cancels the query
     * @return days remaining, or zero if none
     */
    int privileges(CancellationSignal signal);

    /**
     * Gives some of our privileges to another user.
     *
     * @param to who receives them
     * @param days how many days to give
     * @param signal cancels the request
     */
    void giftPrivileges(Username to, int days, CancellationSignal signal);

    /**
     * Changes this account's password.
     *
     * @param newPassword the new password
     * @param signal cancels the request
     */
    void changePassword(String newPassword, CancellationSignal signal);

    /**
     * Tells the server how fast we upload, which is what peers see when ranking
     * sources.
     *
     * @param bytesPerSecond the speed to report
     */
    void reportUploadSpeed(long bytesPerSecond);

    /**
     * Returns the stream of account events.
     *
     * @return the event stream
     */
    EventStream<MeEvent> events();
}
