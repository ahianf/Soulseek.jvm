// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;
import java.util.Optional;

/** Pierces the local firewall to initiate a connection. */
public final class PierceFirewall implements IInitializationMessage {
    private final int token;

    /** Creates a firewall-piercing message. */
    public PierceFirewall(int token) {
        this.token = token;
    }

    /** Returns the connection token. */
    public int getToken() {
        return token;
    }

    /**
     * Attempts to parse a firewall-piercing message.
     *
     * @param bytes the framed message
     * @return the parsed message, or empty when parsing fails
     */
    public static Optional<PierceFirewall> tryFromByteArray(byte[] bytes) {
        try {
            MessageReader<MessageCode.Initialization> reader =
                    new MessageReader<>(bytes, MessageCode.Initialization.class);
            if (reader.readCode() != MessageCode.Initialization.PIERCE_FIREWALL) {
                return Optional.empty();
            }

            return Optional.of(new PierceFirewall(reader.readInteger()));
        } catch (RuntimeException exception) {
            return Optional.empty();
        }
    }

    /** Serializes this message. */
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Initialization.PIERCE_FIREWALL)
                .writeInteger(token)
                .build();
    }
}
