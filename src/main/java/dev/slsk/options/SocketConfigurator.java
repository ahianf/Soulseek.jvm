// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import java.io.IOException;
import java.net.Socket;

/**
 * Configures a newly created TCP socket.
 */
@FunctionalInterface
public interface SocketConfigurator {
    /**
     * Configures a socket.
     *
     * @param socket the socket to configure
     * @throws IOException when socket configuration fails
     */
    void configure(Socket socket) throws IOException;
}
