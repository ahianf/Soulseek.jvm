// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** The two policies the download queue runs on. */
class PolicyTest {

    @Nested
    class RetryTest {

        @Test
        @DisplayName("the default retries only the two refusals that are about this moment")
        void defaultsRetryOnlyTransientRejections() {
            RetryPolicy policy = RetryPolicy.defaults();
            assertEquals(
                    Set.of(RejectionReason.QUEUE_FULL, RejectionReason.PENDING_SHUTDOWN), policy.retryableRejections());

            // A peer that does not share the file will never share it, and one
            // that has banned us will not unban us because we asked again.
            for (RejectionReason reason : RejectionReason.values()) {
                boolean transientRefusal = policy.retryableRejections().contains(reason);
                assertEquals(
                        transientRefusal,
                        policy.shouldRetry(new TransferOutcome.Rejected(reason, "because"), 1),
                        reason + " should " + (transientRefusal ? "" : "not ") + "be retried");
            }
        }

        @Test
        void aFailureIsRetriedOnlyWhenItSaysItIsRetryable() {
            RetryPolicy policy = RetryPolicy.defaults();
            assertTrue(policy.shouldRetry(new TransferOutcome.Failed(new java.io.IOException("reset"), true), 1));
            assertFalse(policy.shouldRetry(new TransferOutcome.Failed(new java.io.IOException("gone"), false), 1));
        }

        @Test
        @DisplayName("a cancellation is a decision, and a success has nothing to try again")
        void neitherCancellationNorSuccessIsRetried() {
            RetryPolicy policy = RetryPolicy.defaults();
            assertFalse(policy.shouldRetry(new TransferOutcome.Cancelled(), 1));
            assertFalse(policy.shouldRetry(new TransferOutcome.Succeeded(1024, Duration.ofSeconds(1)), 1));
        }

        @Test
        void stopsAtMaxAttemptsWhateverTheOutcome() {
            RetryPolicy policy = RetryPolicy.defaults();
            TransferOutcome retryable = new TransferOutcome.Rejected(RejectionReason.QUEUE_FULL, "full");
            assertTrue(policy.shouldRetry(retryable, 1));
            assertTrue(policy.shouldRetry(retryable, 2));
            assertFalse(policy.shouldRetry(retryable, 3), "three attempts means three, not three retries");
        }

        @Test
        @DisplayName("the backoff doubles from the second attempt and stops at the ceiling")
        void backoffGrowsAndIsCapped() {
            RetryPolicy policy = RetryPolicy.defaults();
            assertEquals(Duration.ZERO, policy.backoffBefore(1), "the first attempt does not wait");
            assertEquals(Duration.ofSeconds(5), policy.backoffBefore(2));
            assertEquals(Duration.ofSeconds(10), policy.backoffBefore(3));
            assertEquals(Duration.ofSeconds(20), policy.backoffBefore(4));
            assertEquals(Duration.ofMinutes(5), policy.backoffBefore(20));
        }

        @Test
        void noneNeverRetries() {
            RetryPolicy policy = RetryPolicy.none();
            assertEquals(1, policy.maxAttempts());
            assertFalse(policy.shouldRetry(new TransferOutcome.Rejected(RejectionReason.QUEUE_FULL, "full"), 1));
        }

        @Test
        void rejectsPoliciesThatCannotMeanAnything() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new RetryPolicy(0, Duration.ZERO, 2, Duration.ofMinutes(1), Set.of()));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new RetryPolicy(3, Duration.ofSeconds(-1), 2, Duration.ofMinutes(1), Set.of()));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new RetryPolicy(3, Duration.ofMinutes(2), 2, Duration.ofMinutes(1), Set.of()),
                    "a ceiling below the floor is not a range");
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new RetryPolicy(3, Duration.ofSeconds(1), 0.5, Duration.ofMinutes(1), Set.of()),
                    "a multiplier below one is a backoff that shrinks");
        }
    }

    @Nested
    class DownloadTest {

        @Test
        @DisplayName("the default is one download per peer, because more is how clients get banned")
        void defaultsAreCourteous() {
            DownloadPolicy policy = DownloadPolicy.defaults();
            assertEquals(3, policy.maxConcurrent());
            assertEquals(1, policy.maxConcurrentPerUser());
            assertTrue(policy.speedLimit().isUnlimited());
            assertEquals(Duration.ofSeconds(30), policy.queuePositionPollInterval());
            assertEquals(RetryPolicy.defaults(), policy.retry());
        }

        @Test
        @DisplayName("a per-peer cap above the overall cap is not a policy, it is a typo")
        void rejectsAPerUserCapAboveTheOverallCap() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new DownloadPolicy(
                            2, 3, Bandwidth.unlimited(), Duration.ofSeconds(30), RetryPolicy.defaults()));
        }

        @Test
        void loweringTheOverallCapLowersThePerPeerCapWithIt() {
            DownloadPolicy policy =
                    DownloadPolicy.defaults().maxConcurrentPerUser(3).maxConcurrent(2);
            assertEquals(2, policy.maxConcurrent());
            assertEquals(2, policy.maxConcurrentPerUser());
        }

        @Test
        void witherKeepsEverythingElse() {
            DownloadPolicy policy = DownloadPolicy.defaults()
                    .speedLimit(Bandwidth.ofKibibytesPerSecond(512))
                    .queuePositionPollInterval(Duration.ofMinutes(1))
                    .retry(RetryPolicy.none());

            assertEquals(3, policy.maxConcurrent());
            assertEquals(1, policy.maxConcurrentPerUser());
            assertEquals(Bandwidth.ofKibibytesPerSecond(512), policy.speedLimit());
            assertEquals(Duration.ofMinutes(1), policy.queuePositionPollInterval());
            assertEquals(RetryPolicy.none(), policy.retry());
        }

        @Test
        void rejectsPoliciesThatCannotMeanAnything() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new DownloadPolicy(
                            0, 1, Bandwidth.unlimited(), Duration.ofSeconds(1), RetryPolicy.defaults()));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new DownloadPolicy(
                            1, 0, Bandwidth.unlimited(), Duration.ofSeconds(1), RetryPolicy.defaults()));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new DownloadPolicy(1, 1, Bandwidth.unlimited(), Duration.ZERO, RetryPolicy.defaults()),
                    "polling every no time is a busy loop");
        }
    }
}
