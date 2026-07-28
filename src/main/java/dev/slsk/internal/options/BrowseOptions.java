// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

/**
 * Options for a browse operation.
 */
public class BrowseOptions {
    /** The default response timeout in milliseconds. */
    public static final int DEFAULT_RESPONSE_TIMEOUT = 60_000;

    private final BrowseProgressCallback progressUpdated;
    private final int responseTimeout;

    /**
     * Creates browse options with source defaults.
     */
    public BrowseOptions() {
        this(DEFAULT_RESPONSE_TIMEOUT, null);
    }

    /**
     * Creates browse options with a response timeout.
     *
     * @param responseTimeout the response timeout in milliseconds
     */
    public BrowseOptions(int responseTimeout) {
        this(responseTimeout, null);
    }

    /**
     * Creates browse options.
     *
     * @param responseTimeout the response timeout in milliseconds
     * @param progressUpdated the browse progress callback
     */
    public BrowseOptions(int responseTimeout, BrowseProgressCallback progressUpdated) {
        this.responseTimeout = responseTimeout;
        this.progressUpdated = progressUpdated;
    }

    /**
     * Returns the browse progress callback.
     *
     * @return the callback, or {@code null}
     */
    public final BrowseProgressCallback getProgressUpdated() {
        return progressUpdated;
    }

    /**
     * Returns the response timeout in milliseconds.
     *
     * @return the response timeout
     */
    public final int getResponseTimeout() {
        return responseTimeout;
    }
}
