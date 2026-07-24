// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** A file-search request routed by the server. */
public final class ServerSearchRequest implements IncomingMessage {
    private final String query;
    private final int token;
    private final String username;

    /** Creates a server-routed search request. */
    public ServerSearchRequest(String username, int token, String query) {
        this.username = username;
        this.token = token;
        this.query = query;
    }

    public String getQuery() {
        return query;
    }

    public int getToken() {
        return token;
    }

    public String getUsername() {
        return username;
    }

    /** Parses a server-routed search request. */
    public static ServerSearchRequest fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.FILE_SEARCH, "ServerSearchRequest");
        return new ServerSearchRequest(reader.readString(), reader.readInteger(), reader.readString());
    }
}
