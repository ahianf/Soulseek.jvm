// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

/** Event payload emitted as browse response data is received. */
public record BrowseProgressUpdatedEvent(String username, long bytesTransferred, long size)
        implements SoulseekClientEvent {

    /** Returns the number of bytes remaining. */
    public long bytesRemaining() {
        return size - bytesTransferred;
    }

    /** Returns the completion percentage. */
    public double percentComplete() {
        return (bytesTransferred / (double) size) * 100.0d;
    }
}
