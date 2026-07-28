// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;

/** Notifies a peer that an upload failed. */
public final class UploadFailed implements IncomingMessage, OutgoingMessage {
    private final String filename;

    /** Creates an upload-failed message. */
    public UploadFailed(String filename) {
        this.filename = filename;
    }

    /** Returns the failed filename. */
    public String getFilename() {
        return filename;
    }

    /** Parses an upload-failed message. */
    public static UploadFailed fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(bytes, MessageCode.Peer.class);
        MessageCode.Peer code = reader.readCode();
        if (code != MessageCode.Peer.UPLOAD_FAILED) {
            throw new MessageException("Message Code mismatch creating UploadFailed " + "(expected: 46, received: "
                    + code.getValue() + ")");
        }

        return new UploadFailed(reader.readString());
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Peer.UPLOAD_FAILED)
                .writeString(filename)
                .build();
    }
}
