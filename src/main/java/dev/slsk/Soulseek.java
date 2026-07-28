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
     * Returns a builder.
     *
     * <p>The only way to get a client. Everything but the credentials and the
     * application's minor version has a working default.
     *
     * @return a builder
     */
    static SoulseekBuilder builder() {
        return new SoulseekBuilder();
    }

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
     * What the library is doing, and where it sits on the network.
     *
     * @return the diagnostics facet
     */
    Diagnostics diagnostics();

    /**
     * Chat rooms.
     *
     * @return the rooms facet
     */
    Rooms rooms();

    /**
     * Searching the network.
     *
     * @return the search facet
     */
    Search search();

    /**
     * Downloads, and the queue that runs them.
     *
     * @return the downloads facet
     */
    Downloads downloads();

    /**
     * Uploads peers have asked us for.
     *
     * @return the uploads facet
     */
    Uploads uploads();

    /**
     * What we offer to the network.
     *
     * @return the shares facet
     */
    Shares shares();

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
