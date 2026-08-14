// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.transfer;

/**
 * Why a peer refused a transfer.
 *
 * <p>Soulseek carries this as free text, so every client that wants to tell a
 * user why a download died ends up string-matching {@code "File not shared"} in
 * its own code. This library does that matching once, keeps the original text
 * alongside the classification in {@link TransferOutcome.Rejected}, and leaves
 * the consumer to render a reason rather than parse one.
 *
 * <p>Unrecognised text maps to {@link #UNKNOWN} with the original preserved.
 * <strong>Adding a constant here is not a breaking change</strong>: a consumer
 * must already handle {@code UNKNOWN}, so a reason that used to fall through to
 * it and now has its own name cannot surprise a correct consumer.
 */
public enum RejectionReason {

    /** The peer is not sharing the file. Retrying will not help. */
    FILE_NOT_SHARED,

    /** The peer has banned us. Retrying will not help. */
    BANNED,

    /** The peer caps how many files one user may queue. */
    TOO_MANY_FILES,

    /** The peer caps how many megabytes one user may queue. */
    TOO_MANY_MEGABYTES,

    /** The peer is shutting down. Worth retrying later. */
    PENDING_SHUTDOWN,

    /** The peer's queue is full. Worth retrying later. */
    QUEUE_FULL,

    /** The peer cancelled a transfer that was already agreed. */
    CANCELLED_BY_PEER,

    /** The peer said something this library does not recognise. */
    UNKNOWN
}
