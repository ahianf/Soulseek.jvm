// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import dev.slsk.Progress;
import dev.slsk.TransferId;
import dev.slsk.TransferOutcome;
import dev.slsk.TransferState;
import dev.slsk.Upload;
import dev.slsk.Username;
import java.time.Instant;
import java.util.Objects;

/** What happened to uploads peers asked us for. */
public sealed interface UploadEvent extends SoulseekEvent {

    /** A peer asked for a file and the policy accepted. */
    record Requested(Upload upload, Instant at) implements UploadEvent {
        public Requested {
            Objects.requireNonNull(upload, "upload");
            Objects.requireNonNull(at, "at");
        }
    }

    /** An upload moved between states. */
    record StateChanged(TransferId id, TransferState from, TransferState to, Instant at) implements UploadEvent {
        public StateChanged {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(to, "to");
            Objects.requireNonNull(at, "at");
        }
    }

    /** Bytes moved. Coalesced to a fixed cadence. */
    record Progressed(TransferId id, Progress progress, Instant at) implements UploadEvent {
        public Progressed {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(progress, "progress");
            Objects.requireNonNull(at, "at");
        }
    }

    /** An upload ended. */
    record Finished(TransferId id, TransferOutcome outcome, Instant at) implements UploadEvent {
        public Finished {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(at, "at");
        }
    }

    /** We refused a peer, by policy or by ban. */
    record Denied(Username user, String path, String reason, Instant at) implements UploadEvent {
        public Denied {
            Objects.requireNonNull(user, "user");
            Objects.requireNonNull(path, "path");
            Objects.requireNonNull(reason, "reason");
            Objects.requireNonNull(at, "at");
        }
    }
}
