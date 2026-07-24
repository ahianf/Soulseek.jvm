// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import java.net.InetSocketAddress;

/** Uniquely identifies a TCP connection instance. */
public final class ConnectionKey {
    private final InetSocketAddress ipEndPoint;
    private final String username;

    /** Creates an endpoint-only key. */
    public ConnectionKey(InetSocketAddress ipEndPoint) {
        this(null, ipEndPoint);
    }

    /** Creates a username-and-endpoint key. */
    public ConnectionKey(String username, InetSocketAddress ipEndPoint) {
        this.username = username;
        this.ipEndPoint = ipEndPoint;
    }

    public InetSocketAddress getIpEndPoint() {
        return ipEndPoint;
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
        if (ipEndPoint != null) {
            address = ipEndPoint.getAddress() == null
                    ? ipEndPoint.getHostString()
                    : ipEndPoint.getAddress().getHostAddress();
            port = Integer.toString(ipEndPoint.getPort());
        }
        return (username == null ? "" : username) + ":" + address + ":" + port;
    }
}
