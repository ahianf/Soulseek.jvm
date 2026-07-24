// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;

/** Advises the server of the local listen port. */
public final class SetListenPortCommand extends IntegerServerMessage {
    public SetListenPortCommand(int port) {
        super(MessageCode.Server.SET_LISTEN_PORT, validate(port));
    }

    public int getPort() {
        return value();
    }

    private static int validate(int port) {
        if (port < 1024 || port > 65535) {
            throw new IllegalArgumentException("The port must be between 1024 and 65535");
        }
        return port;
    }
}
