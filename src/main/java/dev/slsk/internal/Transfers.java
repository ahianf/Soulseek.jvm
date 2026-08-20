// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.internal.transfer.Transfer;
import dev.slsk.internal.transfer.TransferDirection;
import dev.slsk.internal.transfer.TransferPhase;
import dev.slsk.internal.transfer.TransferTermination;
import dev.slsk.transfer.Progress;
import dev.slsk.transfer.TransferId;
import dev.slsk.transfer.TransferOutcome;
import dev.slsk.transfer.TransferState;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Translation from the internal {@link dev.slsk.internal.transfer.Transfer} to the 1.0
 * transfer model.
 *
 * <p>Shared by the download and upload facets, because the state mapping is the
 * same in both directions and only the surrounding record differs.
 *
 * <p>The interesting part is the state mapping. Internally a transfer keeps its
 * lifecycle phase separate from queue placement and its terminal reason. Here
 * that representation becomes a sealed hierarchy in which each state carries
 * its own data and nothing else.
 */
final class Transfers {

    private Transfers() {}

    /** Derives an id from the transfer's token, which is unique per transfer. */
    static TransferId id(Transfer transfer) {
        return TransferId.of(transfer.direction() + ":" + transfer.token());
    }

    /**
     * The id an upload holding this token wears — the same string
     * {@link #id(Transfer)} derives once that upload is running.
     *
     * <p>A queued request reserves its token before a slot frees, so the wait
     * and the transfer it becomes are one id rather than two. They were two:
     * the queue named a request {@code UPLOAD:queued:7} and the upload that
     * served it {@code UPLOAD:8301}, so an id a consumer was handed while the
     * request waited stopped resolving the moment it started, and every waiting
     * upload looked like a second transfer that then vanished.
     *
     * @param token the token reserved for the upload
     * @return the id it wears, queued and running alike
     */
    static TransferId uploadId(int token) {
        return TransferId.of(TransferDirection.UPLOAD + ":" + token);
    }

    /**
     * Maps the internal transfer phase onto the sealed hierarchy.
     */
    static TransferState state(Transfer transfer) {
        return state(transfer, transfer.phase());
    }

    /**
     * Maps a specific phase onto the sealed hierarchy, for callers holding a
     * transition's previous phase rather than the transfer's current one.
     */
    static TransferState state(Transfer transfer, TransferPhase source) {
        if (source == null) {
            return new TransferState.Queued(0);
        }
        return switch (source) {
            case COMPLETED -> new TransferState.Finished(outcome(transfer));
            case IN_PROGRESS -> new TransferState.Transferring(progress(transfer));
            case INITIALIZING -> new TransferState.Connecting(false);
            case QUEUED -> new TransferState.QueuedRemotely(java.util.OptionalInt.empty(), Instant.now());
            case REQUESTED -> new TransferState.Requesting();
            case NONE -> new TransferState.Queued(0);
        };
    }

    /**
     * Returns how a finished transfer ended.
     *
     * <p>What the transfer path reports to everything above it. A transfer that
     * is somehow not finished is reported as a failure rather than silently
     * treated as a success, because the only callers are the ones that just ran
     * it to a terminal state.
     *
     * @param transfer the terminal transfer
     * @return its outcome
     */
    static TransferOutcome outcomeOf(Transfer transfer) {
        return state(transfer) instanceof TransferState.Finished finished
                ? finished.outcome()
                : new TransferOutcome.Failed(new IllegalStateException("the transfer did not finish"), true);
    }

    private static TransferOutcome outcome(Transfer transfer) {
        TransferTermination termination = transfer.termination();
        if (termination == null) {
            return new TransferOutcome.Failed(
                    new IllegalStateException("the completed transfer has no termination reason"), true);
        }
        return switch (termination) {
            case SUCCEEDED ->
                new TransferOutcome.Succeeded(
                        transfer.bytesTransferred(),
                        transfer.elapsedTime() == null ? Duration.ZERO : transfer.elapsedTime());
            case CANCELLED -> new TransferOutcome.Cancelled();
            case REJECTED -> {
                Throwable cause = transfer.exception();
                String message = cause == null ? "" : String.valueOf(cause.getMessage());
                yield new TransferOutcome.Rejected(RejectionReasons.parse(message), message);
            }
            case TIMED_OUT ->
                new TransferOutcome.Failed(
                        transfer.exception() == null
                                ? new java.util.concurrent.TimeoutException("the transfer timed out")
                                : transfer.exception(),
                        true);
            case ABORTED ->
                new TransferOutcome.Failed(
                        transfer.exception() == null
                                ? new IllegalStateException("the transfer was aborted")
                                : transfer.exception(),
                        false);
            case ERRORED ->
                new TransferOutcome.Failed(
                        transfer.exception() == null
                                ? new IllegalStateException("the transfer failed")
                                : transfer.exception(),
                        true);
        };
    }

    static Progress progress(Transfer transfer) {
        return Progress.of(transfer.bytesTransferred(), transfer.size(), transfer.averageSpeed());
    }

    static Optional<Instant> startedAt(Transfer transfer) {
        return Optional.ofNullable(transfer.startTime());
    }

    static Optional<Instant> endedAt(Transfer transfer) {
        return Optional.ofNullable(transfer.endTime());
    }
}
