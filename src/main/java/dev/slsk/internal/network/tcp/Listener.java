// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

import dev.slsk.Subscription;
import dev.slsk.internal.options.ConnectionOptions;
import java.net.InetAddress;
import java.util.function.Consumer;

/** Listens for client connections for TCP network services. */
public interface Listener {
    /** Subscribes to accepted connections. */
    Subscription subscribe(Consumer<? super TransportConnection> listener);

    /** Returns options used for accepted connections. */
    ConnectionOptions getConnectionOptions();

    /** Returns the bound IP address. */
    InetAddress getIpAddress();

    /** Returns whether the listener is active. */
    boolean isListening();

    /** Returns the configured port. */
    int getPort();

    /** Starts listening. */
    void start();

    /** Stops listening. */
    void stop();
}
