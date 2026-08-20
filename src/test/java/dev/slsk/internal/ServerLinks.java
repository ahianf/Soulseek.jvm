// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.network.MessageConnection;

/**
 * Builds a logged-in {@link ServerLink} for a test in another package.
 *
 * <p>{@code ServerLink} is public so that the peer and distributed networks can
 * write to the server and name us to a peer. What establishes it — adopting a
 * connection, recording who the login was accepted as — is the engine's alone
 * and is package-private, which leaves a test of those networks unable to build
 * one. This is compiled into the same package and hands one over; it exists so
 * that the two setters do not have to be public for a test's sake.
 */
public final class ServerLinks {

    private ServerLinks() {}

    /**
     * Returns a link that is connected, logged in, and writes to the given
     * connection.
     *
     * @param waiter the correlator
     * @param diagnostic where the link's own diagnostics go
     * @param connection what a write goes out on
     * @param username who the link is logged in as
     * @return the link
     */
    public static ServerLink loggedIn(
            Waiter waiter, DiagnosticSink diagnostic, MessageConnection connection, String username) {
        return over(waiter, diagnostic, connection, username, () -> SoulseekClientState.LOGGED_IN);
    }

    /**
     * Returns a link whose state the caller decides, call by call.
     *
     * @param waiter the correlator
     * @param diagnostic where the link's own diagnostics go
     * @param connection what a write goes out on
     * @param username who the link is logged in as
     * @param state what state the client is in
     * @return the link
     */
    public static ServerLink over(
            Waiter waiter,
            DiagnosticSink diagnostic,
            MessageConnection connection,
            String username,
            java.util.function.Supplier<SoulseekClientState> state) {
        ServerLink link = new ServerLink(waiter, diagnostic, state);
        link.connection(connection);
        link.username(username);
        return link;
    }
}
