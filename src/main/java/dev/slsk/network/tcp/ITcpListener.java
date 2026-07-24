// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import java.net.Socket;
import java.util.concurrent.CompletableFuture;

/** Listens for connections from TCP network clients. */
interface ITcpListener {
    /** Accepts a pending connection request asynchronously. */
    CompletableFuture<Socket> acceptTcpClientAsync();

    /** Returns whether a connection request is pending. */
    boolean pending();

    /** Starts listening for incoming connection requests. */
    void start();

    /** Stops the listener. */
    void stop();
}
