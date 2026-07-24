// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** A distributed file-search request. */
public final class DistributedSearchRequest implements IncomingMessage, OutgoingMessage {
    private final String query;
    private final int token;
    private final String username;

    /** Creates a distributed file-search request. */
    public DistributedSearchRequest(String username, int token, String query) {
        this.username = username;
        this.token = token;
        this.query = query;
    }

    /** Returns the search query. */
    public String getQuery() {
        return query;
    }

    /** Returns the request token. */
    public int getToken() {
        return token;
    }

    /** Returns the requesting username. */
    public String getUsername() {
        return username;
    }

    /**
     * Parses a distributed search request.
     *
     * @param bytes the framed message
     * @return the parsed message
     */
    public static DistributedSearchRequest fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Distributed> reader = new MessageReader<>(bytes, MessageCode.Distributed.class);
        MessageCode.Distributed code = reader.readCode();
        if (code != MessageCode.Distributed.SEARCH_REQUEST) {
            throw new MessageException("Message Code mismatch creating DistributedSearchRequest "
                    + "(expected: 3, received: " + code.getValue() + ")");
        }

        reader.readInteger();
        String parsedUsername = reader.readString();
        int parsedToken = reader.readInteger();
        String parsedQuery = reader.readString();
        return new DistributedSearchRequest(parsedUsername, parsedToken, parsedQuery);
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Distributed.SEARCH_REQUEST)
                .writeInteger(0)
                .writeString(username)
                .writeInteger(token)
                .writeString(query)
                .build();
    }
}
