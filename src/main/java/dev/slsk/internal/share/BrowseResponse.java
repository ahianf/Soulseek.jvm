// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.share;

import dev.slsk.internal.messaging.messages.BrowseResponseFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** A response to a peer browse request. */
public class BrowseResponse {
    private final List<Directory> directories;
    private final int directoryCount;
    private final List<Directory> lockedDirectories;
    private final int lockedDirectoryCount;

    /** Creates an empty browse response. */
    public BrowseResponse() {
        this(null, null);
    }

    /** Creates a browse response with unlocked directories. */
    public BrowseResponse(Iterable<? extends Directory> directoryList) {
        this(directoryList, null);
    }

    /** Creates a browse response. */
    public BrowseResponse(
            Iterable<? extends Directory> directoryList, Iterable<? extends Directory> lockedDirectoryList) {
        directories = immutableCopy(directoryList);
        directoryCount = directories.size();
        lockedDirectories = immutableCopy(lockedDirectoryList);
        lockedDirectoryCount = lockedDirectories.size();
    }

    /** Returns the unlocked-directory snapshot. */
    public List<Directory> getDirectories() {
        return directories;
    }

    /** Returns the unlocked-directory count. */
    public int getDirectoryCount() {
        return directoryCount;
    }

    /** Returns the locked-directory snapshot. */
    public List<Directory> getLockedDirectories() {
        return lockedDirectories;
    }

    /** Returns the locked-directory count. */
    public int getLockedDirectoryCount() {
        return lockedDirectoryCount;
    }

    /** Serializes this response to its peer protocol message. */
    public byte[] toByteArray() {
        return BrowseResponseFactory.toByteArray(this);
    }

    private static List<Directory> immutableCopy(Iterable<? extends Directory> source) {
        List<Directory> copy = new ArrayList<>();
        if (source != null) {
            source.forEach(copy::add);
        }
        return Collections.unmodifiableList(copy);
    }
}
