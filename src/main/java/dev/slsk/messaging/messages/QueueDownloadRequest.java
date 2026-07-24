// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** Requests that a peer queue a file for download. */
public final class QueueDownloadRequest implements IncomingMessage, OutgoingMessage {
    private final String filename;

    /** Creates a queue-download request. */
    public QueueDownloadRequest(String filename) {
        this.filename = filename;
    }

    /** Returns the filename being enqueued. */
    public String getFilename() {
        return filename;
    }

    /** Parses a queue-download request. */
    public static QueueDownloadRequest fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(bytes, MessageCode.Peer.class);
        MessageCode.Peer code = reader.readCode();
        if (code != MessageCode.Peer.QUEUE_DOWNLOAD) {
            throw new MessageException("Message Code mismatch creating QueueDownloadRequest "
                    + "(expected: 43, received: " + code.getValue() + ")");
        }

        return new QueueDownloadRequest(reader.readString());
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Peer.QUEUE_DOWNLOAD)
                .writeString(filename)
                .build();
    }
}
