// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;

/** Notifies a peer that an upload was denied. */
public final class UploadDenied implements IncomingMessage, OutgoingMessage {
    private final String filename;
    private final String message;

    /** Creates an upload-denied message. */
    public UploadDenied(String filename, String message) {
        this.filename = filename;
        this.message = message;
    }

    /** Returns the denied filename. */
    public String getFilename() {
        return filename;
    }

    /** Returns the denial reason. */
    public String getMessage() {
        return message;
    }

    /** Parses an upload-denied message. */
    public static UploadDenied fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(bytes, MessageCode.Peer.class);
        MessageCode.Peer code = reader.readCode();
        if (code != MessageCode.Peer.UPLOAD_DENIED) {
            throw new MessageException("Message Code mismatch creating UploadDenied " + "(expected: 50, received: "
                    + code.getValue() + ")");
        }

        String parsedFilename = reader.readString();
        String parsedMessage = reader.readString();
        return new UploadDenied(parsedFilename, parsedMessage);
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Peer.UPLOAD_DENIED)
                .writeString(filename)
                .writeString(message)
                .build();
    }
}
