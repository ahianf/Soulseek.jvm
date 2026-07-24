// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

/**
 * User statistics.
 */
public class UserStatistics {
    private final int averageSpeed;
    private final int directoryCount;
    private final int fileCount;
    private final long uploadCount;
    private final String username;

    /**
     * Creates user statistics.
     *
     * @param username the username
     * @param averageSpeed the average upload speed
     * @param uploadCount the tracked upload count
     * @param fileCount the shared file count
     * @param directoryCount the shared directory count
     */
    public UserStatistics(String username, int averageSpeed, long uploadCount, int fileCount, int directoryCount) {
        this.username = username;
        this.averageSpeed = averageSpeed;
        this.uploadCount = uploadCount;
        this.fileCount = fileCount;
        this.directoryCount = directoryCount;
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
