// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

import java.net.Socket;

/** Accepts client sockets from a listening endpoint. */
public interface SocketAcceptor {
    /**
     * Accepts a pending connection request, blocking until one arrives.
     *
     * <p>The caller owns a virtual thread of its own — the accept loop is
     * nothing else — so dispatching the accept onto a second thread and
     * blocking on the result bought a thread and a future per connection and
     * cost the loop the ability to tell its own {@code stop()} from a fault.
     *
     * @return the accepted socket
     */
    Socket accept();

    /** Returns whether a connection request is pending. */
    boolean pending();

    /** Starts listening for incoming connection requests. */
    void start();

    /** Stops the listener. */
    void stop();
}
