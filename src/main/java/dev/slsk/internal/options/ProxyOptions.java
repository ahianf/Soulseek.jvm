// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import dev.slsk.exceptions.AddressException;
import dev.slsk.internal.common.CommonUtils;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/** SOCKS proxy configuration. */
public record ProxyOptions(
        String address,
        int port,
        String username,
        String password,
        InetAddress ipAddress,
        InetSocketAddress ipEndpoint) {

    public ProxyOptions(String address, int port) {
        this(address, port, null, null);
    }

    public ProxyOptions(String address, int port, String username, String password) {
        this(address, port, username, password, resolve(address, port, username, password));
    }

    private ProxyOptions(String address, int port, String username, String password, Resolved resolved) {
        this(address, port, username, password, resolved.address(), resolved.endpoint());
    }

    private static Resolved resolve(String address, int port, String username, String password) {
        if (CommonUtils.isNullOrUnicodeWhitespace(address)) {
            throw new IllegalArgumentException("address must contain non-whitespace text");
        }
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 0 and 65535: " + port);
        }
        if ((username == null) != (password == null)) {
            throw new IllegalArgumentException("username and password must both be specified");
        }
        if (username != null) {
            if (username.isEmpty() || username.length() > 255) {
                throw new IllegalArgumentException("username length must be between 1 and 255");
            }
            if (password.isEmpty() || password.length() > 255) {
                throw new IllegalArgumentException("password length must be between 1 and 255");
            }
        }

        try {
            InetAddress resolvedAddress = InetAddress.getAllByName(address)[0];
            return new Resolved(resolvedAddress, new InetSocketAddress(resolvedAddress, port));
        } catch (UnknownHostException exception) {
            throw new AddressException(
                    "Failed to resolve address '" + address + "': " + exception.getMessage(), exception);
        }
    }

    private record Resolved(InetAddress address, InetSocketAddress endpoint) {}
}
