// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

/**
 * Event arguments raised when a peer denies a download.
 */
public class DownloadDeniedEvent extends UserEvent {
    private final String filename;
    private final String message;

    /**
     * Creates download-denied event payload.
     *
     * @param username the associated username
     * @param filename the associated filename
     * @param message the denial message
     */
    public DownloadDeniedEvent(String username, String filename, String message) {
        super(username);
        this.filename = filename;
        this.message = message;
    }

    /**
     * Returns the associated filename.
     *
     * @return the filename
     */
    public final String getFilename() {
        return filename;
    }

    /**
     * Returns the denial message.
     *
     * @return the message
     */
    public final String getMessage() {
        return message;
    }
}
