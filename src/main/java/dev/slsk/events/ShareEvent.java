// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import dev.slsk.ShareIndex;
import dev.slsk.Username;
import java.time.Instant;
import java.util.Objects;

/** What happened to our shares. */
public sealed interface ShareEvent extends SoulseekEvent {

    /** A scan began. */
    record ScanStarted(Instant at) implements ShareEvent {
        public ScanStarted {
            Objects.requireNonNull(at, "at");
        }
    }

    /** A scan made progress. */
    record ScanProgressed(int directoriesScanned, int filesFound, Instant at) implements ShareEvent {
        public ScanProgressed {
            Objects.requireNonNull(at, "at");
        }
    }

    /** A scan finished, and the counts were announced to the server. */
    record ScanCompleted(ShareIndex index, Instant at) implements ShareEvent {
        public ScanCompleted {
            Objects.requireNonNull(index, "index");
            Objects.requireNonNull(at, "at");
        }
    }

    /** We served a peer's browse request. */
    record BrowseServed(Username to, int fileCount, Instant at) implements ShareEvent {
        public BrowseServed {
            Objects.requireNonNull(to, "to");
            Objects.requireNonNull(at, "at");
        }
    }
}
