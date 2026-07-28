// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

/**
 * A Soulseek client.
 *
 * <p>Ten facets and {@code close()}. That is the entire root type, and it
 * replaces one interface carrying two hundred members, ninety-two of which were
 * listener registrations.
 *
 * <p>A method lives on the facet that owns <em>the state it changes</em>, not
 * the noun it mentions. {@code ban} names a user but changes our own upload
 * policy, so it is on {@code uploads()}. {@code giftPrivileges} names a user but
 * spends our own privilege balance, so it is on {@code me()}. Applied
 * consistently, the facet is guessable without reading documentation.
 *
 * <p>The remaining facets — {@code search()}, {@code downloads()}, {@code
 * uploads()}, {@code rooms()}, {@code shares()} and {@code diagnostics()} — are
 * added as their phases land. See {@code JAVA_API_1_0_GOAL.md}.
 *
 * <p>Every facet answers "what is true now?" synchronously and cheaply, and
 * publishes events as deltas on that. A consumer that misses every event and
 * polls is degraded, not broken; one starting cold never needs event history.
 * Where the initial read and the subscription must agree exactly, the facet
 * offers {@code attach}, which does both under one lock.
 *
 * <p>Build one for the lifetime of the process and never replace it. It outlives
 * every socket it opens: reconnection, re-subscribing watched users and
 * re-announcing shares all happen underneath.
 */
public interface Soulseek extends AutoCloseable {

    /**
     * The connection to the server.
     *
     * @return the connection facet
     */
    Connection connection();

    /**
     * Private messages.
     *
     * @return the chat facet
     */
    Chat chat();

    /**
     * This account.
     *
     * @return the account facet
     */
    Me me();

    /**
     * Other users.
     *
     * @return the users facet
     */
    Users users();

    /**
     * Closes the client and everything it owns — connections, listeners, timers,
     * waiters, and transfers in flight.
     *
     * <p>Idempotent, and does not throw. Narrows {@link AutoCloseable#close()}
     * to remove the checked exception, because a consumer closing a client in a
     * finally block has nothing useful to do with one.
     */
    @Override
    void close();
}
