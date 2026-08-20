// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.share.SharedDirectory;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** The response to a peer folder-contents request. */
public record FolderContentsResponse(int token, String directoryName, List<SharedDirectory> directories)
        implements IncomingMessage, OutgoingMessage {
    public FolderContentsResponse {
        Objects.requireNonNull(directories, "directories");
        directories = List.copyOf(directories);
    }

    /** Returns the number of directories. */
    public int directoryCount() {
        return directories.size();
    }

    /** Parses a compressed folder-contents response. */
    public static FolderContentsResponse fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(bytes, MessageCode.Peer.class);
        MessageCode.Peer code = reader.readCode();
        if (code != MessageCode.Peer.FOLDER_CONTENTS_RESPONSE) {
            throw new MessageException("Message Code mismatch creating FolderContentsResponse "
                    + "(expected: 37, received: " + code.getValue());
        }

        reader.decompress();
        int parsedToken = reader.readInteger();
        String parsedRoot = reader.readString();
        int parsedCount = reader.readInteger();
        List<SharedDirectory> parsedDirectories = new ArrayList<>();
        for (int index = 0; index < parsedCount; index++) {
            parsedDirectories.add(reader.readDirectory());
        }
        return new FolderContentsResponse(parsedToken, parsedRoot, parsedDirectories);
    }

    @Override
    public byte[] toByteArray() {
        MessageBuilder builder = new MessageBuilder()
                .writeCode(MessageCode.Peer.FOLDER_CONTENTS_RESPONSE)
                .writeInteger(token)
                .writeString(directoryName)
                .writeInteger(directoryCount());
        for (SharedDirectory directory : directories) {
            builder.writeDirectory(directory);
        }
        return builder.compress().build();
    }
}
