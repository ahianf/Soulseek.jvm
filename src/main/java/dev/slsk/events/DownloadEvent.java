// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import dev.slsk.Download;
import dev.slsk.Progress;
import dev.slsk.TransferId;
import dev.slsk.TransferOutcome;
import dev.slsk.TransferState;
import java.time.Instant;
import java.util.Objects;
import java.util.OptionalInt;

/** What happened to our downloads. */
public sealed interface DownloadEvent extends SoulseekEvent {

    /** A download joined the queue. */
    record Enqueued(Download download, Instant at) implements DownloadEvent {
        public Enqueued {
            Objects.requireNonNull(download, "download");
            Objects.requireNonNull(at, "at");
        }
    }

    /** A download moved between states. */
    record StateChanged(TransferId id, TransferState from, TransferState to, Instant at) implements DownloadEvent {
        public StateChanged {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(to, "to");
            Objects.requireNonNull(at, "at");
        }
    }

    /**
     * Bytes moved.
     *
     * <p>Coalesced to a fixed cadence rather than raised per socket read, and
     * carrying a rate the library has already smoothed. A UI must never be the
     * thing that throttles the network.
     */
    record Progressed(TransferId id, Progress progress, Instant at) implements DownloadEvent {
        public Progressed {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(progress, "progress");
            Objects.requireNonNull(at, "at");
        }
    }

    /** Our place in the peer's queue changed, or was reported for the first time. */
    record QueuePositionChanged(TransferId id, OptionalInt position, Instant at) implements DownloadEvent {
        public QueuePositionChanged {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(position, "position");
            Objects.requireNonNull(at, "at");
        }
    }

    /** A download ended. */
    record Finished(TransferId id, TransferOutcome outcome, Instant at) implements DownloadEvent {
        public Finished {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(at, "at");
        }
    }

    /** A failed download will be tried again. */
    record RetryScheduled(TransferId id, int attempt, Instant nextAttemptAt, Instant at) implements DownloadEvent {
        public RetryScheduled {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(nextAttemptAt, "nextAttemptAt");
            Objects.requireNonNull(at, "at");
        }
    }

    /** A finished download was dropped from the list. */
    record Forgotten(TransferId id, Instant at) implements DownloadEvent {
        public Forgotten {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(at, "at");
        }
    }
}
