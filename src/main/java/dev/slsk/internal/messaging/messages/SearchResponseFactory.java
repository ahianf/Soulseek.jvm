// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.search.SearchResponseMessage;
import dev.slsk.internal.share.File;
import java.util.List;
import java.util.Objects;

/** Serializes and parses search-response messages. */
public final class SearchResponseFactory {
    private SearchResponseFactory() {}

    /** Parses a compressed search response. */
    public static SearchResponseMessage fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(bytes, MessageCode.Peer.class);
        MessageCode.Peer code = reader.readCode();
        if (code != MessageCode.Peer.SEARCH_RESPONSE) {
            throw new MessageException("Message Code mismatch creating SearchResponseMessage "
                    + "(expected: 9, received: " + code.getValue());
        }

        reader.decompress();
        String username = reader.readString();
        int token = reader.readInteger();
        List<File> files = reader.readFiles(reader.readInteger());
        boolean hasFreeUploadSlot = reader.readByte() > 0;
        int uploadSpeed = reader.readInteger();
        int queueLength = reader.readInteger();
        if (reader.hasMoreData()) {
            reader.readInteger();
        }
        List<File> lockedFiles = List.of();
        if (reader.hasMoreData()) {
            lockedFiles = reader.readFiles(reader.readInteger());
        }
        return new SearchResponseMessage(
                username, token, hasFreeUploadSlot, uploadSpeed, queueLength, files, lockedFiles);
    }

    /** Serializes a search response. */
    public static byte[] toByteArray(SearchResponseMessage searchResponse) {
        Objects.requireNonNull(searchResponse, "searchResponse");
        MessageBuilder builder = new MessageBuilder()
                .writeCode(MessageCode.Peer.SEARCH_RESPONSE)
                .writeString(searchResponse.username())
                .writeInteger(searchResponse.token())
                .writeInteger(searchResponse.fileCount());
        for (File file : searchResponse.files()) {
            builder.writeFile(file);
        }
        builder.writeByte(searchResponse.hasFreeUploadSlot() ? 1 : 0)
                .writeInteger(searchResponse.uploadSpeed())
                .writeInteger(searchResponse.queueLength())
                .writeInteger(0)
                .writeInteger(searchResponse.lockedFileCount());
        for (File file : searchResponse.lockedFiles()) {
            builder.writeFile(file);
        }
        return builder.compress().build();
    }
}
