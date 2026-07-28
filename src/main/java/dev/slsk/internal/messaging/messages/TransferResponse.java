// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;

/** A peer response to a transfer request. */
public final class TransferResponse implements IncomingMessage, OutgoingMessage {
    private final long fileSize;
    private final boolean isAllowed;
    private final String message;
    private final int token;

    /** Creates a denied transfer response. */
    public TransferResponse(int token, String message) {
        this.token = token;
        isAllowed = false;
        this.message = message;
        fileSize = 0;
    }

    /** Creates an allowed transfer response with a file size. */
    public TransferResponse(int token, long fileSize) {
        this.token = token;
        isAllowed = true;
        this.fileSize = fileSize;
        message = null;
    }

    /** Creates an allowed transfer response with the default file size. */
    public TransferResponse(int token) {
        this(token, 0L);
    }

    /** Returns whether the transfer is allowed. */
    public boolean isAllowed() {
        return isAllowed;
    }

    /** Returns the file size, or zero when absent or denied. */
    public long getFileSize() {
        return fileSize;
    }

    /** Returns the denial reason, or null when allowed. */
    public String getMessage() {
        return message;
    }

    /** Returns the transfer token. */
    public int getToken() {
        return token;
    }

    /** Parses a transfer response. */
    public static TransferResponse fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(bytes, MessageCode.Peer.class);
        MessageCode.Peer code = reader.readCode();
        if (code != MessageCode.Peer.TRANSFER_RESPONSE) {
            throw new MessageException("Message Code mismatch creating TransferResponse " + "(expected: 41, received: "
                    + code.getValue() + ")");
        }

        int parsedToken = reader.readInteger();
        boolean allowed = reader.readByte() == 1;
        if (allowed && reader.hasMoreData()) {
            return new TransferResponse(parsedToken, reader.readLong());
        } else if (!allowed) {
            return new TransferResponse(parsedToken, reader.readString());
        }
        return new TransferResponse(parsedToken);
    }

    @Override
    public byte[] toByteArray() {
        MessageBuilder builder = new MessageBuilder()
                .writeCode(MessageCode.Peer.TRANSFER_RESPONSE)
                .writeInteger(token)
                .writeByte(isAllowed ? 1 : 0);
        if (isAllowed) {
            builder.writeLong(fileSize);
        } else {
            builder.writeString(message);
        }
        return builder.build();
    }
}
