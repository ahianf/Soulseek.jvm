// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import dev.slsk.options.ConnectionOptions;
import java.net.InetAddress;

/** Listens for client connections for TCP network services. */
public interface IListener {
    /** Adds an accepted-connection listener. */
    void addAcceptedListener(ListenerAcceptedEventListener listener);

    /** Removes an accepted-connection listener. */
    void removeAcceptedListener(ListenerAcceptedEventListener listener);

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
