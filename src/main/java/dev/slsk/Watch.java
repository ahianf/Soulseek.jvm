// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

/**
 * An open subscription to a user's status.
 *
 * <p>This is an {@link AutoCloseable} rather than a {@code watchUser} /
 * {@code unwatchUser} pair because Soulseek's {@code AddUser} is a
 * <em>server-side</em> subscription that dies with the connection. Two things
 * follow, and applications get both wrong.
 *
 * <p>First, watches have to be re-registered on every login, or status updates
 * simply stop arriving after the first reconnect and nothing says so. The
 * library re-adds them; a consumer holding a {@code Watch} keeps working across
 * a disconnect without knowing one happened.
 *
 * <p>Second, two parts of an application watching the same user share one
 * server-side subscription, so whichever unwatches first silently breaks the
 * other. Watches are reference-counted here, and the subscription is dropped
 * only when the last one closes.
 */
public interface Watch extends AutoCloseable {

    /**
     * Returns the user being watched.
     *
     * @return the user
     */
    Username user();

    /**
     * Returns the user's status as last reported.
     *
     * @return the status
     */
    UserStatus status();

    /**
     * Releases this watch. Idempotent, and never throws.
     *
     * <p>The server-side subscription survives until every watch on the user is
     * closed.
     */
    @Override
    void close();
}
