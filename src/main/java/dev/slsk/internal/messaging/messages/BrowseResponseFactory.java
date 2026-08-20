// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.share.BrowseResponse;
import dev.slsk.internal.share.Directory;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Serializes and parses browse-response messages. */
public final class BrowseResponseFactory {
    private BrowseResponseFactory() {}

    /** Parses a compressed browse response. */
    public static BrowseResponse fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(bytes, MessageCode.Peer.class);
        MessageCode.Peer code = reader.readCode();
        if (code != MessageCode.Peer.BROWSE_RESPONSE) {
            throw new MessageException(
                    "Message Code mismatch creating BrowseResponse (expected: 5, received: " + code.getValue() + ")");
        }

        reader.decompress();
        int directoryCount = reader.readInteger();
        List<Directory> directories = new ArrayList<>();
        List<Directory> lockedDirectories = new ArrayList<>();
        for (int index = 0; index < directoryCount; index++) {
            directories.add(reader.readDirectory());
        }
        if (reader.hasMoreData()) {
            reader.readInteger();
            if (reader.hasMoreData()) {
                int lockedCount = reader.readInteger();
                for (int index = 0; index < lockedCount; index++) {
                    lockedDirectories.add(reader.readDirectory());
                }
            }
        }
        return new BrowseResponse(directories, lockedDirectories);
    }

    /** Serializes a browse response. */
    public static byte[] toByteArray(BrowseResponse browseResponse) {
        Objects.requireNonNull(browseResponse, "browseResponse");
        MessageBuilder builder = new MessageBuilder()
                .writeCode(MessageCode.Peer.BROWSE_RESPONSE)
                .writeInteger(browseResponse.directoryCount());
        for (Directory directory : browseResponse.directories()) {
            builder.writeDirectory(directory);
        }
        builder.writeInteger(0).writeInteger(browseResponse.lockedDirectoryCount());
        for (Directory directory : browseResponse.lockedDirectories()) {
            builder.writeDirectory(directory);
        }
        return builder.compress().build();
    }
}
