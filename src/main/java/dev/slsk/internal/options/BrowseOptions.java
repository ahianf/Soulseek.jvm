// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import java.time.Duration;

/** Options for a browse operation. */
public record BrowseOptions(Duration responseTimeout, BrowseProgressCallback progressUpdated) {
    /** The default response timeout. */
    public static final Duration DEFAULT_RESPONSE_TIMEOUT = Duration.ofMinutes(1);

    /** Creates browse options with defaults. */
    public BrowseOptions() {
        this(DEFAULT_RESPONSE_TIMEOUT, null);
    }

    /** Starts a field-named browse-options builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for browse options. */
    public static final class Builder {
        private BrowseProgressCallback progressUpdated;
        private Duration responseTimeout = DEFAULT_RESPONSE_TIMEOUT;

        public Builder responseTimeout(Duration value) {
            responseTimeout = value;
            return this;
        }

        public Builder progressUpdated(BrowseProgressCallback value) {
            progressUpdated = value;
            return this;
        }

        public BrowseOptions build() {
            return new BrowseOptions(responseTimeout, progressUpdated);
        }
    }
}
