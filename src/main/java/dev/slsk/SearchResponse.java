// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.messaging.messages.SearchResponseFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** A response to a file search. */
public class SearchResponse {
    private final int fileCount;
    private final List<File> files;
    private final boolean hasFreeUploadSlot;
    private final int lockedFileCount;
    private final List<File> lockedFiles;
    private final int queueLength;
    private final int token;
    private final int uploadSpeed;
    private final String username;

    /** Creates a search response without locked files. */
    public SearchResponse(
            String username,
            int token,
            boolean hasFreeUploadSlot,
            int uploadSpeed,
            int queueLength,
            Iterable<? extends File> fileList) {
        this(username, token, hasFreeUploadSlot, uploadSpeed, queueLength, fileList, null);
    }

    /** Creates a search response. */
    public SearchResponse(
            String username,
            int token,
            boolean hasFreeUploadSlot,
            int uploadSpeed,
            int queueLength,
            Iterable<? extends File> fileList,
            Iterable<? extends File> lockedFileList) {
        this.username = username;
        this.token = token;
        this.uploadSpeed = uploadSpeed;
        this.queueLength = queueLength;
        this.hasFreeUploadSlot = hasFreeUploadSlot;
        files = immutableCopy(fileList);
        fileCount = files.size();
        lockedFiles = immutableCopy(lockedFileList);
        lockedFileCount = lockedFiles.size();
    }

    SearchResponse(
            SearchResponse searchResponse, Iterable<? extends File> fileList, Iterable<? extends File> lockedFileList) {
        this(
                searchResponse.username,
                searchResponse.token,
                searchResponse.hasFreeUploadSlot,
                searchResponse.uploadSpeed,
                searchResponse.queueLength,
                fileList,
                lockedFileList);
    }

    /** Returns the file count. */
    public int getFileCount() {
        return fileCount;
    }

    /** Returns the immutable file snapshot. */
    public List<File> getFiles() {
        return files;
    }

    /** Returns whether the peer has a free upload slot. */
    public boolean hasFreeUploadSlot() {
        return hasFreeUploadSlot;
    }

    /** Returns the locked-file count. */
    public int getLockedFileCount() {
        return lockedFileCount;
    }

    /** Returns the immutable locked-file snapshot. */
    public List<File> getLockedFiles() {
        return lockedFiles;
    }

    /** Returns the peer's upload queue length. */
    public int getQueueLength() {
        return queueLength;
    }

    /** Returns the search token. */
    public int getToken() {
        return token;
    }

    /** Returns the peer's upload speed. */
    public int getUploadSpeed() {
        return uploadSpeed;
    }

    /** Returns the responding peer's username. */
    public String getUsername() {
        return username;
    }

    /** Serializes this response to its peer protocol message. */
    public byte[] toByteArray() {
        return SearchResponseFactory.toByteArray(this);
    }

    private static List<File> immutableCopy(Iterable<? extends File> source) {
        List<File> copy = new ArrayList<>();
        if (source != null) {
            source.forEach(copy::add);
        }
        return Collections.unmodifiableList(copy);
    }
}
