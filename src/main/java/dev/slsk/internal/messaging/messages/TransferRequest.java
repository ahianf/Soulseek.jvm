// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.TransferDirection;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import java.util.Objects;

/** Requests a file transfer from a peer. */
public final class TransferRequest implements IncomingMessage, OutgoingMessage {
    private final TransferDirection direction;
    private final String filename;
    private final long fileSize;
    private final int token;

    /** Creates a transfer request with the source default file size. */
    public TransferRequest(TransferDirection direction, int token, String filename) {
        this(direction, token, filename, 0);
    }

    /** Creates a transfer request. */
    public TransferRequest(TransferDirection direction, int token, String filename, long fileSize) {
        this.direction = Objects.requireNonNull(direction, "direction");
        this.token = token;
        this.filename = filename;
        this.fileSize = fileSize;
    }

    /** Returns the transfer direction. */
    public TransferDirection getDirection() {
        return direction;
    }

    /** Returns the remote filename. */
    public String getFilename() {
        return filename;
    }

    /** Returns the remote file size. */
    public long getFileSize() {
        return fileSize;
    }

    /** Returns the transfer token. */
    public int getToken() {
        return token;
    }

    /** Parses a transfer request. */
    public static TransferRequest fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(bytes, MessageCode.Peer.class);
        MessageCode.Peer code = reader.readCode();
        if (code != MessageCode.Peer.TRANSFER_REQUEST) {
            throw new MessageException("Message Code mismatch creating TransferRequest " + "(expected: 40, received: "
                    + code.getValue() + ")");
        }

        TransferDirection parsedDirection = TransferDirection.fromValue(reader.readInteger());
        int parsedToken = reader.readInteger();
        String parsedFilename = reader.readString();
        long parsedFileSize = reader.hasMoreData() ? reader.readLong() : 0;
        return new TransferRequest(parsedDirection, parsedToken, parsedFilename, parsedFileSize);
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Peer.TRANSFER_REQUEST)
                .writeInteger(direction.getValue())
                .writeInteger(token)
                .writeString(filename)
                .writeLong(fileSize)
                .build();
    }
}
