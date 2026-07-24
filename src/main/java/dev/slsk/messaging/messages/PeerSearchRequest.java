// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** A search request received directly from a peer. */
public final class PeerSearchRequest implements IIncomingMessage {
    private final String query;
    private final int token;

    /** Creates a peer-search request. */
    public PeerSearchRequest(int token, String query) {
        this.token = token;
        this.query = query;
    }

    /** Returns the search query. */
    public String getQuery() {
        return query;
    }

    /** Returns the search token. */
    public int getToken() {
        return token;
    }

    /** Parses a peer-search request. */
    public static PeerSearchRequest fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(bytes, MessageCode.Peer.class);
        MessageCode.Peer code = reader.readCode();
        if (code != MessageCode.Peer.SEARCH_REQUEST) {
            throw new MessageException("Message Code mismatch creating PeerSearchRequest " + "(expected: 8, received: "
                    + code.getValue() + ")");
        }

        int parsedToken = reader.readInteger();
        String parsedQuery = reader.readString();
        return new PeerSearchRequest(parsedToken, parsedQuery);
    }
}
