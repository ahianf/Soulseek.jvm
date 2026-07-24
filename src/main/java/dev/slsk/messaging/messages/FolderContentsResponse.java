// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.Directory;
import dev.slsk.exceptions.MessageException;
import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** The response to a peer folder-contents request. */
public final class FolderContentsResponse implements IncomingMessage, OutgoingMessage {
    private final int directoryCount;
    private final String directoryName;
    private final List<Directory> directories;
    private final int token;

    /** Creates a folder-contents response. */
    public FolderContentsResponse(int token, String directoryName, Iterable<? extends Directory> directories) {
        this.token = token;
        this.directoryName = directoryName;
        Objects.requireNonNull(directories, "directories");
        List<Directory> copy = new ArrayList<>();
        directories.forEach(copy::add);
        this.directories = Collections.unmodifiableList(copy);
        directoryCount = copy.size();
    }

    /** Returns the immutable directory snapshot. */
    public List<Directory> getDirectories() {
        return directories;
    }

    /** Returns the number of directories. */
    public int getDirectoryCount() {
        return directoryCount;
    }

    /** Returns the requested root-directory name. */
    public String getDirectoryName() {
        return directoryName;
    }

    /** Returns the response token. */
    public int getToken() {
        return token;
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
        List<Directory> parsedDirectories = new ArrayList<>();
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
                .writeInteger(directoryCount);
        for (Directory directory : directories) {
            builder.writeDirectory(directory);
        }
        return builder.compress().build();
    }
}
