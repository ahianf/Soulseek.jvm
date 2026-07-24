// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.Objects;

/**
 * User data returned by the server.
 */
public class UserData {
    private final int averageSpeed;
    private final String countryCode;
    private final int directoryCount;
    private final int fileCount;
    private final Integer slotsFree;
    private final UserPresence status;
    private final long uploadCount;
    private final String username;

    /**
     * Creates user data without a free-slot count.
     *
     * @param username the username
     * @param status the user's presence
     * @param averageSpeed the average upload speed
     * @param uploadCount the tracked upload count
     * @param fileCount the shared file count
     * @param directoryCount the shared directory count
     * @param countryCode the country code
     */
    public UserData(
            String username,
            UserPresence status,
            int averageSpeed,
            long uploadCount,
            int fileCount,
            int directoryCount,
            String countryCode) {
        this(username, status, averageSpeed, uploadCount, fileCount, directoryCount, countryCode, null);
    }

    /**
     * Creates user data.
     *
     * @param username the username
     * @param status the user's presence
     * @param averageSpeed the average upload speed
     * @param uploadCount the tracked upload count
     * @param fileCount the shared file count
     * @param directoryCount the shared directory count
     * @param countryCode the country code
     * @param slotsFree the number of free download slots, if supplied
     */
    public UserData(
            String username,
            UserPresence status,
            int averageSpeed,
            long uploadCount,
            int fileCount,
            int directoryCount,
            String countryCode,
            Integer slotsFree) {
        this.username = username;
        this.status = Objects.requireNonNull(status, "status");
        this.averageSpeed = averageSpeed;
        this.uploadCount = uploadCount;
        this.fileCount = fileCount;
        this.directoryCount = directoryCount;
        this.slotsFree = slotsFree;
        this.countryCode = countryCode;
    }

    /**
     * Returns the average upload speed.
     *
     * @return the average upload speed
     */
    public final int getAverageSpeed() {
        return averageSpeed;
    }

    /**
     * Returns the country code.
     *
     * @return the country code
     */
    public final String getCountryCode() {
        return countryCode;
    }

    /**
     * Returns the shared directory count.
     *
     * @return the directory count
     */
    public final int getDirectoryCount() {
        return directoryCount;
    }

    /**
     * Returns the shared file count.
     *
     * @return the file count
     */
    public final int getFileCount() {
        return fileCount;
    }

    /**
     * Returns the number of free download slots, if supplied.
     *
     * @return the free-slot count, or {@code null}
     */
    public final Integer getSlotsFree() {
        return slotsFree;
    }

    /**
     * Returns the user's presence.
     *
     * @return the user's presence
     */
    public final UserPresence getStatus() {
        return status;
    }

    /**
     * Returns the tracked upload count.
     *
     * @return the upload count
     */
    public final long getUploadCount() {
        return uploadCount;
    }

    /**
     * Returns the username.
     *
     * @return the username
     */
    public final String getUsername() {
        return username;
    }
}
