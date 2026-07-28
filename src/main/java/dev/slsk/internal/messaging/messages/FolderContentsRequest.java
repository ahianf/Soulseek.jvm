// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;

/** Requests the contents of a directory from a peer. */
public final class FolderContentsRequest implements IncomingMessage, OutgoingMessage {
    private final String directoryName;
    private final int token;

    /** Creates a folder-contents request. */
    public FolderContentsRequest(int token, String directoryName) {
        this.directoryName = directoryName;
        this.token = token;
    }

    /** Returns the directory to fetch. */
    public String getDirectoryName() {
        return directoryName;
    }

    /** Returns the request token. */
    public int getToken() {
        return token;
    }

    /** Parses a folder-contents request. */
    public static FolderContentsRequest fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(bytes, MessageCode.Peer.class);
        MessageCode.Peer code = reader.readCode();
        if (code != MessageCode.Peer.FOLDER_CONTENTS_REQUEST) {
            throw new MessageException("Message Code mismatch creating FolderContentsRequest "
                    + "(expected: 36, received: " + code.getValue() + ")");
        }

        int parsedToken = reader.readInteger();
        String parsedDirectoryName = reader.readString();
        return new FolderContentsRequest(parsedToken, parsedDirectoryName);
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Peer.FOLDER_CONTENTS_REQUEST)
                .writeInteger(token)
                .writeString(directoryName)
                .build();
    }
}
