// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.RejectionReason;
import dev.slsk.user.UserStatistics;
import dev.slsk.user.Username;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The standard policy, which is a pure function and can be read as one.
 *
 * <p>That is the point of the shape. Four callbacks used to answer parts of this
 * question and none could see the others, so "two slots, one per user,
 * privileged first" was not something an application could state — it had to be
 * assembled from four functions and a shadow queue.
 */
class UploadPolicyTest {

    private static final Username ALICE = Username.of("alice");

    /** A context stated as five numbers, because that is all a decision needs. */
    private record Context(boolean privileged, long sent, int activeSlots, int queueDepth, int activeForRequester)
            implements UploadContext {

        @Override
        public UserStatistics requesterStatistics() {
            return new UserStatistics(ALICE, 0, 0, 0, 0);
        }

        @Override
        public boolean requesterIsPrivileged() {
            return privileged;
        }

        @Override
        public long bytesAlreadySentTo(Username user) {
            return sent;
        }

        @Override
        public int activeSlotsForRequester() {
            return activeForRequester;
        }
    }

    private static UploadPolicy.Decision decide(UploadPolicy policy, Context context) {
        return policy.decide(new UploadRequest(ALICE, "music\\song.mp3", 1024), context);
    }

    @Test
    @DisplayName("a free slot is served now")
    void aFreeSlotIsAllowed() {
        UploadPolicy policy = UploadPolicy.standard(2, 1);
        assertInstanceOf(UploadPolicy.Decision.Allow.class, decide(policy, new Context(false, 0, 0, 0, 0)));
        assertInstanceOf(UploadPolicy.Decision.Allow.class, decide(policy, new Context(false, 0, 1, 0, 0)));
    }

    @Test
    @DisplayName("a full slot list queues, behind whoever is already waiting")
    void afullSlotListQueues() {
        UploadPolicy policy = UploadPolicy.standard(2, 1);
        UploadPolicy.Decision decision = decide(policy, new Context(false, 0, 2, 3, 0));
        assertEquals(new UploadPolicy.Decision.Queue(4), decision);
    }

    /**
     * Protocol-mandated rather than a matter of taste: a user who paid for
     * privileges expects to jump queues, and a client that ignores that is one
     * people stop downloading from.
     */
    @Test
    @DisplayName("a privileged user goes to the front of the queue")
    void privilegedUsersJumpTheQueue() {
        UploadPolicy policy = UploadPolicy.standard(2, 1);
        assertEquals(new UploadPolicy.Decision.Queue(1), decide(policy, new Context(true, 0, 2, 9, 0)));
        assertEquals(new UploadPolicy.Decision.Queue(10), decide(policy, new Context(false, 0, 2, 9, 0)));
    }

    @Test
    @DisplayName("a peer already being served waits rather than being refused")
    void thePerUserCapQueuesRatherThanDenies() {
        UploadPolicy policy = UploadPolicy.standard(4, 1);
        // The file is there and they can have it; they simply already have our
        // attention, which is their own doing and not a refusal.
        assertEquals(new UploadPolicy.Decision.Queue(1), decide(policy, new Context(false, 0, 1, 0, 1)));
    }

    @Test
    void refuseAllDeniesWithAReasonAPeersClientUnderstands() {
        UploadPolicy.Decision decision = decide(UploadPolicy.refuseAll(), new Context(false, 0, 0, 0, 0));
        UploadPolicy.Decision.Deny denied = assertInstanceOf(UploadPolicy.Decision.Deny.class, decision);
        assertEquals(RejectionReason.FILE_NOT_SHARED, denied.reason());
    }

    @Test
    void rejectsPoliciesAndDecisionsThatCannotMeanAnything() {
        assertThrows(IllegalArgumentException.class, () -> UploadPolicy.standard(0, 1));
        assertThrows(IllegalArgumentException.class, () -> UploadPolicy.standard(1, 0));
        assertThrows(IllegalArgumentException.class, () -> new UploadPolicy.Decision.Queue(0));
        assertThrows(NullPointerException.class, () -> new UploadPolicy.Decision.Deny(null, "why"));
        assertThrows(NullPointerException.class, () -> new UploadPolicy.Decision.Deny(RejectionReason.BANNED, null));
        assertThrows(IllegalArgumentException.class, () -> new UploadRequest(ALICE, "path", -1));
    }
}
