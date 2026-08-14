// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.upload;

import dev.slsk.share.RemotePath;
import dev.slsk.transfer.Priority;
import dev.slsk.transfer.TransferId;
import dev.slsk.transfer.TransferState;
import dev.slsk.user.Username;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * An upload, as it stands.
 *
 * <p>Uploads are asked for by a peer rather than started by us, so there is no
 * enqueue and nothing to tag: what we know about one is what the peer's request
 * said. Cancelling and reprioritising are ours to do; everything else about
 * whether it happens at all is the upload policy's decision.
 *
 * @param id identifies this upload
 * @param user who asked for it
 * @param path the file they asked for
 * @param size its size in bytes
 * @param state where it is now
 * @param priority its place in our queue
 * @param requestedAt when the peer asked
 * @param startedAt when bytes first moved
 * @param endedAt when it finished, one way or another
 */
public record Upload(
        TransferId id,
        Username user,
        String path,
        long size,
        TransferState state,
        Priority priority,
        Instant requestedAt,
        Optional<Instant> startedAt,
        Optional<Instant> endedAt) {

    /** Validates and returns the upload. */
    public Upload {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(priority, "priority");
        Objects.requireNonNull(requestedAt, "requestedAt");
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(endedAt, "endedAt");
    }

    /**
     * Returns the file name, without the directories above it.
     *
     * @return the last path segment
     */
    public String name() {
        return RemotePath.basename(path);
    }

    /**
     * Returns whether this upload is over.
     *
     * @return {@code true} if terminal
     */
    public boolean isFinished() {
        return state.isTerminal();
    }
}
