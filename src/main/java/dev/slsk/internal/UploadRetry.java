// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-FileCopyrightText: Nicotine+ Contributors
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.Username;
import dev.slsk.internal.common.Scheduler;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Re-offers an upload whose peer never came back for it.
 *
 * <p>When an upload fails retryably — a handshake timeout, a dropped
 * connection — the library tells the peer with {@code UploadFailed}, and a
 * well-behaved client re-queues the file itself: that fresh request is the
 * normal recovery, and it needs nothing from us. This class covers the peer
 * that does not re-ask, because its client timed out on its own side or the
 * failure was ours. Without it a retryable failure is terminal exactly when
 * the other side is not paying attention.
 *
 * <p>Nicotine+ answers the same gap with a 180-second sweep that re-enqueues
 * every upload still marked {@code CONNECTION_TIMEOUT}. The cadence is kept;
 * the mechanics diverge:
 *
 * <ul>
 *   <li><strong>One-shot per failure, not a sweep.</strong> A failure books a
 *       single re-offer 180 seconds out. Nicotine+ can afford a sweep because
 *       its failed transfers stay in its transfer list; here a failed upload
 *       leaves every collection, so there is nothing to sweep — the booking is
 *       the record.
 *   <li><strong>Bounded.</strong> Nicotine+ retries for as long as the row
 *       exists. A library cannot grow state per unresponsive peer for ever, so
 *       after {@value #MAX_ATTEMPTS} failed attempts at one file the record is
 *       dropped and the peer's next request starts fresh.
 *   <li><strong>Keyed on the outcome, not a status.</strong> Nicotine+ matches
 *       the {@code CONNECTION_TIMEOUT} status string. Here the trigger is
 *       {@code TransferOutcome.Failed(retryable=true)}, which also covers the
 *       peer-unreachable case that surfaces as {@code NoResponseException}
 *       rather than a timeout.
 * </ul>
 *
 * <p>The re-offer itself goes back through {@code UploadAdmission}, so a
 * retried upload is re-policed and re-banned exactly like a fresh request; this
 * class decides only <em>when</em> to ask again, never whether it is allowed.
 */
final class UploadRetry {

    /** How long after a retryable failure the file is re-offered. */
    static final Duration DELAY = Duration.ofSeconds(180);

    /** Failed attempts at one file before the record is dropped. */
    static final int MAX_ATTEMPTS = 3;

    /** Where a booked re-offer lands when it comes due. */
    @FunctionalInterface
    interface Reoffer {
        /** Offers the file to the admission again. */
        void reoffer(Username user, String path);
    }

    private final Scheduler scheduler;
    private final Duration delay;
    private final int maxAttempts;
    private final Reoffer reoffer;
    private final DiagnosticSink diagnostic;

    /** Failed attempts per file, cleared on success or on giving up. */
    private final Map<PeerFile, Integer> attempts = new ConcurrentHashMap<>();

    UploadRetry(Scheduler scheduler, Duration delay, int maxAttempts, Reoffer reoffer, DiagnosticSink diagnostic) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.delay = Objects.requireNonNull(delay, "delay");
        this.maxAttempts = maxAttempts;
        this.reoffer = Objects.requireNonNull(reoffer, "reoffer");
        this.diagnostic = Objects.requireNonNull(diagnostic, "diagnostic");
    }

    /**
     * Records a retryable failure and books a re-offer, unless this file has
     * already used up its attempts.
     *
     * <p>Failures count whoever initiated the attempt — a peer's own re-request
     * that fails again is the same file still struggling, and it spends from
     * the same budget.
     *
     * @param user who the upload was for
     * @param path the file that failed
     */
    void failed(Username user, String path) {
        PeerFile file = new PeerFile(user, path);
        int made = attempts.merge(file, 1, Integer::sum);
        if (made > maxAttempts) {
            attempts.remove(file);
            diagnostic.debug("Giving up on re-offering " + path + " to " + user + " after " + made + " attempts");
            return;
        }
        diagnostic.debug("Re-offering " + path + " to " + user + " in " + delay.toSeconds() + "s (attempt " + made
                + " of " + maxAttempts + ")");
        scheduler.schedule(() -> reoffer.reoffer(user, path), delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Clears a file's failure record; the next failure starts a fresh budget. */
    void succeeded(Username user, String path) {
        attempts.remove(new PeerFile(user, path));
    }

    /**
     * Drops a file's record without retrying — the admission denied the
     * re-offer, so asking again on a timer would just be denied again.
     */
    void abandoned(Username user, String path) {
        attempts.remove(new PeerFile(user, path));
    }
}
