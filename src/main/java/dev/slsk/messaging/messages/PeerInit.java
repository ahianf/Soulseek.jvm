// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;
import java.util.Optional;

/** Initiates a peer connection. */
public final class PeerInit implements InitializationMessage {
    private final String connectionType;
    private final int token;
    private final String username;

    /** Creates a peer-initialization message. */
    public PeerInit(String username, String connectionType, int token) {
        this.username = username;
        this.connectionType = connectionType;
        this.token = token;
    }

    /** Returns the connection type, conventionally {@code P} or {@code F}. */
    public String getConnectionType() {
        return connectionType;
    }

    /** Returns the connection token. */
    public int getToken() {
        return token;
    }

    /** Returns the peer username. */
    public String getUsername() {
        return username;
    }

    /**
     * Attempts to parse a peer-initialization message.
     *
     * @param bytes the framed message
     * @return the parsed message, or empty when parsing fails
     */
    public static Optional<PeerInit> tryFromByteArray(byte[] bytes) {
        try {
            MessageReader<MessageCode.Initialization> reader =
                    new MessageReader<>(bytes, MessageCode.Initialization.class);
            if (reader.readCode() != MessageCode.Initialization.PEER_INIT) {
                return Optional.empty();
            }

            String parsedUsername = reader.readString();
            String parsedConnectionType = reader.readString();
            int parsedToken = reader.readInteger();
            return Optional.of(new PeerInit(parsedUsername, parsedConnectionType, parsedToken));
        } catch (RuntimeException exception) {
            return Optional.empty();
        }
    }

    /** Serializes this message. */
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Initialization.PEER_INIT)
                .writeString(username)
                .writeString(connectionType)
                .writeInteger(token)
                .build();
    }
}
