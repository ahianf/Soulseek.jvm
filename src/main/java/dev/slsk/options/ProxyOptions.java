// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import dev.slsk.exceptions.AddressException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * SOCKS proxy configuration.
 */
public class ProxyOptions {
    private final String address;
    private final InetAddress ipAddress;
    private final InetSocketAddress ipEndPoint;
    private final String password;
    private final int port;
    private final String username;

    /**
     * Creates proxy options without credentials.
     *
     * @param address the proxy address
     * @param port the proxy port
     */
    public ProxyOptions(String address, int port) {
        this(address, port, null, null);
    }

    /**
     * Creates proxy options with a username.
     *
     * <p>This overload is retained for the C# optional-parameter call shape;
     * validation requires a password whenever the username is non-null.</p>
     *
     * @param address the proxy address
     * @param port the proxy port
     * @param username the proxy username
     */
    public ProxyOptions(String address, int port, String username) {
        this(address, port, username, null);
    }

    /**
     * Creates proxy options.
     *
     * @param address the proxy address
     * @param port the proxy port
     * @param username the proxy username
     * @param password the proxy password
     */
    public ProxyOptions(String address, int port, String username, String password) {
        if (isNullOrWhiteSpace(address)) {
            throw new IllegalArgumentException(
                    "Address must not be a null or empty string, or one consisting only of whitespace");
        }
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("The port must be within the range 0-65535 (specified: " + port + ")");
        }
        if ((username == null) != (password == null)) {
            throw new IllegalArgumentException("Username and password must both be specified");
        }
        if (username != null) {
            if (username.isEmpty() || username.length() > 255) {
                throw new IllegalArgumentException("The username must be between 1 and 255 characters");
            }
            if (password.isEmpty() || password.length() > 255) {
                throw new IllegalArgumentException("The password must be between 1 and 255 characters");
            }
        }

        InetAddress resolvedAddress;
        try {
            resolvedAddress = InetAddress.getAllByName(address)[0];
        } catch (UnknownHostException exception) {
            // The source interpolates Address before assigning it, yielding
            // an empty address in this message. Preserve that behavior.
            throw new AddressException("Failed to resolve address '': " + exception.getMessage(), exception);
        }

        this.address = address;
        this.ipAddress = resolvedAddress;
        this.port = port;
        this.ipEndPoint = new InetSocketAddress(resolvedAddress, port);
        this.username = username;
        this.password = password;
    }

    /**
     * Returns the configured proxy address.
     *
     * @return the address
     */
    public final String getAddress() {
        return address;
    }

    /**
     * Returns the resolved proxy address.
     *
     * @return the resolved address
     */
    public final InetAddress getIpAddress() {
        return ipAddress;
    }

    /**
     * Returns the resolved proxy endpoint.
     *
     * @return the resolved endpoint
     */
    public final InetSocketAddress getIpEndPoint() {
        return ipEndPoint;
    }

    /**
     * Returns the proxy password.
     *
     * @return the password, or {@code null}
     */
    public final String getPassword() {
        return password;
    }

    /**
     * Returns the proxy port.
     *
     * @return the port
     */
    public final int getPort() {
        return port;
    }

    /**
     * Returns the proxy username.
     *
     * @return the username, or {@code null}
     */
    public final String getUsername() {
        return username;
    }

    private static boolean isNullOrWhiteSpace(String value) {
        if (value == null || value.isEmpty()) {
            return true;
        }
        return value.codePoints()
                .allMatch(codePoint -> Character.isWhitespace(codePoint) || Character.isSpaceChar(codePoint));
    }
}
