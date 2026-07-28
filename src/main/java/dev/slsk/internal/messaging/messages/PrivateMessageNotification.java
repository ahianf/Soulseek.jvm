// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import java.time.Instant;

/** An incoming private message. */
public final class PrivateMessageNotification implements IncomingMessage {
    private final int id;
    private final String message;
    private final boolean replayed;
    private final Instant timestamp;
    private final String username;

    /** Creates a private-message notification. */
    public PrivateMessageNotification(int id, Instant timestamp, String username, String message, boolean replayed) {
        this.id = id;
        this.timestamp = timestamp;
        this.username = username;
        this.message = message;
        this.replayed = replayed;
    }

    public int getId() {
        return id;
    }

    public String getMessage() {
        return message;
    }

    public boolean isReplayed() {
        return replayed;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public String getUsername() {
        return username;
    }

    /** Parses a private-message notification. */
    public static PrivateMessageNotification fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.PRIVATE_MESSAGE, "PrivateMessageNotification");
        int id = reader.readInteger();
        Instant timestamp = Instant.ofEpochSecond(reader.readInteger());
        String username = reader.readString();
        String message = reader.readString();
        boolean replayed = reader.readByte() != 1;
        return new PrivateMessageNotification(id, timestamp, username, message, replayed);
    }
}
