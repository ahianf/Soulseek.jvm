// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import java.net.InetSocketAddress;

/**
 * Event arguments raised when a distributed child connection changes.
 */
public class DistributedChildEventArgs extends SoulseekClientEventArgs {
    private final InetSocketAddress ipEndpoint;
    private final String username;

    /**
     * Creates distributed-child event arguments.
     *
     * @param username the username associated with the connection
     * @param ipEndpoint the connection endpoint
     */
    public DistributedChildEventArgs(String username, InetSocketAddress ipEndpoint) {
        this.username = username;
        this.ipEndpoint = ipEndpoint;
    }

    /**
     * Returns the connection endpoint.
     *
     * @return the IP endpoint
     */
    public final InetSocketAddress getIpEndpoint() {
        return ipEndpoint;
    }

    /**
     * Returns the username associated with the connection.
     *
     * @return the username
     */
    public final String getUsername() {
        return username;
    }
}
