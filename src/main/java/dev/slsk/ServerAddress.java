// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.Objects;

/**
 * Where the Soulseek server lives.
 *
 * <p>Pairing the host with the port makes {@code connect(address, signal)} one
 * argument instead of two, which matters because the two-argument form is a
 * {@code String} beside an {@code int} and reads identically whichever way round
 * a caller writes it.
 *
 * @param host the hostname
 * @param port the port
 */
public record ServerAddress(String host, int port) {

    /** The public server, and the port it has always been on. */
    private static final ServerAddress DEFAULT = new ServerAddress("vps.slsknet.org", 2271);

    /**
     * Validates and returns the address.
     *
     * @throws NullPointerException if {@code host} is {@code null}
     * @throws IllegalArgumentException if {@code host} is blank or {@code port}
     *     is outside 1–65535
     */
    public ServerAddress {
        Objects.requireNonNull(host, "host");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535: " + port);
        }
    }

    /**
     * Returns the public Soulseek server.
     *
     * @return the default address
     */
    public static ServerAddress soulseek() {
        return DEFAULT;
    }

    /**
     * Returns an address.
     *
     * @param host the hostname
     * @param port the port
     * @return the address
     */
    public static ServerAddress of(String host, int port) {
        return new ServerAddress(host, port);
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
