// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

/**
 * Event arguments raised when a peer reports that a download failed.
 */
public class DownloadFailedEventArgs extends UserEventArgs {
    private final String filename;

    /**
     * Creates download-failed event arguments.
     *
     * @param username the associated username
     * @param filename the associated filename
     */
    public DownloadFailedEventArgs(String username, String filename) {
        super(username);
        this.filename = filename;
    }

    /**
     * Returns the associated filename.
     *
     * @return the filename
     */
    public final String getFilename() {
        return filename;
    }
}
