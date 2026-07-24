// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

/**
 * Event arguments raised as browse response data is received.
 */
public class BrowseProgressUpdatedEventArgs extends BrowseEventArgs {
    private final long bytesTransferred;
    private final long bytesRemaining;
    private final double percentComplete;
    private final long size;

    /**
     * Creates browse-progress event arguments.
     *
     * @param username the user associated with the event
     * @param bytesTransferred the total number of transferred bytes
     * @param size the total expected data length
     */
    public BrowseProgressUpdatedEventArgs(String username, long bytesTransferred, long size) {
        super(username);
        this.bytesTransferred = bytesTransferred;
        this.size = size;
        this.bytesRemaining = size - bytesTransferred;
        this.percentComplete = (bytesTransferred / (double) size) * 100.0d;
    }

    /**
     * Returns the total number of transferred bytes.
     *
     * @return the transferred bytes
     */
    public final long getBytesTransferred() {
        return bytesTransferred;
    }

    /**
     * Returns the number of bytes remaining.
     *
     * @return the remaining bytes
     */
    public final long getBytesRemaining() {
        return bytesRemaining;
    }

    /**
     * Returns the completion percentage.
     *
     * @return the completion percentage
     */
    public final double getPercentComplete() {
        return percentComplete;
    }

    /**
     * Returns the total expected data length.
     *
     * @return the total size
     */
    public final long getSize() {
        return size;
    }
}
