// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import java.net.InetSocketAddress;

/** Uniquely identifies a TCP connection instance. */
public final class ConnectionKey {
    private final InetSocketAddress ipEndpoint;
    private final String username;

    /** Creates an endpoint-only key. */
    public ConnectionKey(InetSocketAddress ipEndpoint) {
        this(null, ipEndpoint);
    }

    /** Creates a username-and-endpoint key. */
    public ConnectionKey(String username, InetSocketAddress ipEndpoint) {
        this.username = username;
        this.ipEndpoint = ipEndpoint;
    }

    public InetSocketAddress getIpEndpoint() {
        return ipEndpoint;
    }

    public String getUsername() {
        return username;
    }

    /**
     * Compares keys by hash, preserving the source implementation.
     */
    @Override
    public boolean equals(Object other) {
        return other instanceof ConnectionKey key && hashCode() == key.hashCode();
    }

    /**
     * Hashes the source's colon-joined nullable fields.
     */
    @Override
    public int hashCode() {
        return sourceString().hashCode();
    }

    private String sourceString() {
        String address = "";
        String port = "";
        if (ipEndpoint != null) {
            address = ipEndpoint.getAddress() == null
                    ? ipEndpoint.getHostString()
                    : ipEndpoint.getAddress().getHostAddress();
            port = Integer.toString(ipEndpoint.getPort());
        }
        return (username == null ? "" : username) + ":" + address + ":" + port;
    }
}
