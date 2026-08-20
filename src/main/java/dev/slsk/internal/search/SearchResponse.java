// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import dev.slsk.internal.messaging.messages.SearchResponseFactory;
import dev.slsk.internal.share.File;
import java.util.List;

/** A response to a file search. */
public record SearchResponse(
        String username,
        int token,
        boolean hasFreeUploadSlot,
        int uploadSpeed,
        int queueLength,
        List<File> files,
        List<File> lockedFiles) {
    public SearchResponse {
        files = files == null ? List.of() : List.copyOf(files);
        lockedFiles = lockedFiles == null ? List.of() : List.copyOf(lockedFiles);
    }

    /** Creates a search response without locked files. */
    public SearchResponse(
            String username, int token, boolean hasFreeUploadSlot, int uploadSpeed, int queueLength, List<File> files) {
        this(username, token, hasFreeUploadSlot, uploadSpeed, queueLength, files, List.of());
    }

    /** Creates a copy with replacement file lists. */
    SearchResponse(SearchResponse source, List<File> files, List<File> lockedFiles) {
        this(
                source.username,
                source.token,
                source.hasFreeUploadSlot,
                source.uploadSpeed,
                source.queueLength,
                files,
                lockedFiles);
    }

    /** Returns the file count. */
    public int fileCount() {
        return files.size();
    }

    /** Returns the locked-file count. */
    public int lockedFileCount() {
        return lockedFiles.size();
    }

    /** Serializes this response to its peer protocol message. */
    public byte[] toByteArray() {
        return SearchResponseFactory.toByteArray(this);
    }
}
