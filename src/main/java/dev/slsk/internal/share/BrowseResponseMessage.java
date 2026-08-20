// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.share;

import dev.slsk.internal.messaging.messages.BrowseResponseFactory;
import java.util.List;

/** A response to a peer browse request. */
public record BrowseResponseMessage(List<SharedDirectory> directories, List<SharedDirectory> lockedDirectories) {
    public BrowseResponseMessage {
        directories = directories == null ? List.of() : List.copyOf(directories);
        lockedDirectories = lockedDirectories == null ? List.of() : List.copyOf(lockedDirectories);
    }

    /** Creates an empty browse response. */
    public BrowseResponseMessage() {
        this(List.of(), List.of());
    }

    /** Creates a browse response with unlocked directories. */
    public BrowseResponseMessage(List<SharedDirectory> directories) {
        this(directories, List.of());
    }

    /** Returns the unlocked-directory count. */
    public int directoryCount() {
        return directories.size();
    }

    /** Returns the locked-directory count. */
    public int lockedDirectoryCount() {
        return lockedDirectories.size();
    }

    /** Serializes this response to its peer protocol message. */
    public byte[] toByteArray() {
        return BrowseResponseFactory.toByteArray(this);
    }
}
