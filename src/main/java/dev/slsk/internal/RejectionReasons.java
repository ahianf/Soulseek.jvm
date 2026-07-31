// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.RejectionReason;
import java.util.Locale;

/**
 * Classifies the free text a peer sends when it refuses a transfer.
 *
 * <p>Soulseek has no code for this: the peer sends a sentence. Every client that
 * wants to tell a user why a download died therefore string-matches, and this is
 * that matching done once, in the library, rather than in every application.
 *
 * <p>The strings come from what the reference clients actually emit. Anything
 * unrecognised is {@link RejectionReason#UNKNOWN} and the original text is kept
 * alongside it, so nothing is lost and a reason can be given a name later
 * without having discarded the evidence.
 */
final class RejectionReasons {

    private RejectionReasons() {}

    /**
     * Classifies a refusal message.
     *
     * @param message what the peer said; may be {@code null}
     * @return the classification, never {@code null}
     */
    static RejectionReason parse(String message) {
        if (message == null || message.isBlank()) {
            return RejectionReason.UNKNOWN;
        }
        String text = message.toLowerCase(Locale.ROOT);
        if (text.contains("file not shared") || text.contains("not shared")) {
            return RejectionReason.FILE_NOT_SHARED;
        }
        if (text.contains("banned") || text.contains("blocked")) {
            return RejectionReason.BANNED;
        }
        if (text.contains("too many megabytes")) {
            return RejectionReason.TOO_MANY_MEGABYTES;
        }
        if (text.contains("too many files")) {
            return RejectionReason.TOO_MANY_FILES;
        }
        // "User limit of x megabytes exceeded" and "User limit of x files
        // exceeded" say the same two things in the words Nicotine+ used before
        // 3.1.1. Deprecated, still on the wire from anyone who has not
        // upgraded, and the difference matters: both are refusals that expire
        // on their own, and the queue waits them out rather than giving up.
        if (text.contains("user limit of")) {
            return text.contains("megabytes") ? RejectionReason.TOO_MANY_MEGABYTES : RejectionReason.TOO_MANY_FILES;
        }
        if (text.contains("pending shutdown")) {
            return RejectionReason.PENDING_SHUTDOWN;
        }
        if (text.contains("queue full") || text.contains("queue is full")) {
            return RejectionReason.QUEUE_FULL;
        }
        if (text.contains("cancelled") || text.contains("canceled") || text.contains("aborted")) {
            return RejectionReason.CANCELLED_BY_PEER;
        }
        return RejectionReason.UNKNOWN;
    }
}
