// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.user.Username;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A download, as it stands.
 *
 * <p>{@code tags} looks like a convenience and is not. Without it an application
 * that wants to know which album a file belongs to keeps a side table keyed on
 * {@code (user, path)} — which goes stale the moment the same path is enqueued
 * again, since the second enqueue matches the first one's key. Attaching the
 * data at enqueue time and receiving it back on every event removes the side
 * table and the bug in it.
 *
 * @param id identifies this enqueue, not this file
 * @param user who we are downloading from
 * @param path the file on the peer
 * @param size the expected size in bytes, or {@code 0} if the peer did not say
 * @param state where it is now
 * @param priority its place in our own queue
 * @param enqueuedAt when it was enqueued
 * @param startedAt when bytes first moved
 * @param endedAt when it finished, one way or another
 * @param attempt which attempt this is, counting from one
 * @param tags whatever the application attached at enqueue time
 */
public record Download(
        TransferId id,
        Username user,
        String path,
        long size,
        TransferState state,
        Priority priority,
        Instant enqueuedAt,
        Optional<Instant> startedAt,
        Optional<Instant> endedAt,
        int attempt,
        Map<String, String> tags) {

    /** Validates and returns the download. */
    public Download {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(priority, "priority");
        Objects.requireNonNull(enqueuedAt, "enqueuedAt");
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(endedAt, "endedAt");
        tags = Map.copyOf(Objects.requireNonNull(tags, "tags"));
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
     * Returns whether this download is over.
     *
     * @return {@code true} if terminal
     */
    public boolean isFinished() {
        return state.isTerminal();
    }
}
