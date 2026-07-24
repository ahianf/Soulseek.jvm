// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;

/** A bidirectional server ping. */
public final class ServerPing extends EmptyServerMessage implements IncomingMessage {

    /** Creates a server ping. */
    public ServerPing() {
        super(MessageCode.Server.PING);
    }

    /** Parses a server ping. */
    public static ServerPing fromByteArray(byte[] bytes) {
        ServerMessageParser.reader(bytes, MessageCode.Server.PING, "ServerPing");
        return new ServerPing();
    }
}
