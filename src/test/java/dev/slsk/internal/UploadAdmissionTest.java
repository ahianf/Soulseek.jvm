// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.RejectionReason;
import dev.slsk.Username;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.transfer.TransferInternal;
import dev.slsk.spi.UploadPolicy;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What surrounds a policy: the ban that no policy may override, the queue a
 * place-in-queue answer refers to, and the guard around a policy that throws.
 */
class UploadAdmissionTest {

    private static final Username ALICE = Username.of("alice");
    private static final Username BOB = Username.of("bob");

    private final AtomicReference<UploadPolicy> policy = new AtomicReference<>(UploadPolicy.standard(2, 1));
    private final AtomicReference<Map<Integer, TransferInternal>> uploads = new AtomicReference<>(Map.of());
    private final AtomicReference<String> warned = new AtomicReference<>();

    private final UploadAdmission admission = new UploadAdmission(new UploadAdmission.Host() {
        @Override
        public UploadPolicy uploadPolicy() {
            return policy.get();
        }

        @Override
        public Map<Integer, TransferInternal> uploads() {
            return uploads.get();
        }

        @Override
        public boolean isPrivileged(String username) {
            return "vip".equals(username);
        }

        @Override
        public DiagnosticSink diagnostic() {
            return new DiagnosticSink() {
                @Override
                public void trace(String message) {}

                @Override
                public void trace(String message, Throwable exception) {}

                @Override
                public void debug(String message) {}

                @Override
                public void debug(String message, Throwable exception) {}

                @Override
                public void info(String message) {}

                @Override
                public void warning(String message) {
                    warned.set(message);
                }

                @Override
                public void warning(String message, Throwable exception) {
                    warned.set(message);
                }
            };
        }
    });

    /**
     * A ban is an invariant rather than a default. Checking it inside the
     * standard policy would mean a consumer replacing that policy silently
     * unbanned everyone, which is not a thing an application should be able to
     * do by accident.
     */
    @Test
    @DisplayName("a banned user is refused ahead of the policy, whatever the policy says")
    void bansAreCheckedBeforeThePolicy() {
        policy.set((request, context) -> new UploadPolicy.Decision.Allow());

        admission.ban(ALICE, "you know what you did");
        UploadPolicy.Decision decision = admission.decide(ALICE, "music\\song.mp3");

        UploadPolicy.Decision.Deny denied = assertInstanceOf(UploadPolicy.Decision.Deny.class, decision);
        assertEquals(RejectionReason.BANNED, denied.reason());
        assertEquals("you know what you did", denied.message());

        assertInstanceOf(
                UploadPolicy.Decision.Allow.class,
                admission.decide(BOB, "music\\song.mp3"),
                "banning one user must not refuse another");
    }

    @Test
    void banAndUnbanAreIdempotent() {
        admission.ban(ALICE, "reason");
        admission.ban(ALICE, "reason");
        assertEquals(Map.of(ALICE, "reason"), admission.banned());

        admission.unban(ALICE);
        admission.unban(ALICE);
        assertEquals(Map.of(), admission.banned());
        assertInstanceOf(UploadPolicy.Decision.Allow.class, admission.decide(ALICE, "music\\song.mp3"));
    }

    @Test
    @DisplayName("a ban with no reason still tells the peer something")
    void aBanAlwaysCarriesAMessage() {
        admission.ban(ALICE, null);
        UploadPolicy.Decision.Deny denied =
                assertInstanceOf(UploadPolicy.Decision.Deny.class, admission.decide(ALICE, "music\\song.mp3"));
        assertTrue(!denied.message().isBlank());
    }

    @Test
    @DisplayName("a queued peer has a place; anyone else has none")
    void theQueueIsWhereAPlaceComesFrom() {
        policy.set((request, context) -> new UploadPolicy.Decision.Queue(7));
        admission.decide(ALICE, "music\\song.mp3");
        assertEquals(7, admission.place(ALICE, "music\\song.mp3"));

        assertNull(admission.place(ALICE, "music\\other.mp3"), "a file we are not holding has no place");
        assertNull(admission.place(BOB, "music\\song.mp3"), "and neither does another peer");

        // Allowing them takes them out of the queue: they are being served.
        policy.set((request, context) -> new UploadPolicy.Decision.Allow());
        admission.decide(ALICE, "music\\song.mp3");
        assertNull(admission.place(ALICE, "music\\song.mp3"));
    }

    @Test
    void forgetDropsAQueuedRequest() {
        policy.set((request, context) -> new UploadPolicy.Decision.Queue(1));
        admission.decide(ALICE, "music\\song.mp3");
        admission.forget(ALICE, "music\\song.mp3");
        assertNull(admission.place(ALICE, "music\\song.mp3"));
    }

    /**
     * A peer waiting on a read that never completes is worse than a refusal:
     * their client will sit there until it times out and then treat us as
     * broken.
     */
    @Test
    @DisplayName("a policy that throws refuses rather than dropping the request")
    void aThrowingPolicyStillAnswers() {
        policy.set((request, context) -> {
            throw new IllegalStateException("policy is broken");
        });

        UploadPolicy.Decision.Deny denied =
                assertInstanceOf(UploadPolicy.Decision.Deny.class, admission.decide(ALICE, "music\\song.mp3"));
        assertEquals(RejectionReason.UNKNOWN, denied.reason());
        assertTrue(warned.get() != null && warned.get().contains("upload policy"));
    }

    @Test
    @DisplayName("a policy returning nothing is treated as a policy that failed")
    void aNullDecisionIsARefusal() {
        policy.set((request, context) -> null);
        assertInstanceOf(UploadPolicy.Decision.Deny.class, admission.decide(ALICE, "music\\song.mp3"));
    }

    @Test
    void theContextReportsWhatWeHaveSent() {
        AtomicReference<Long> seen = new AtomicReference<>();
        policy.set((request, context) -> {
            seen.set(context.bytesAlreadySentTo(ALICE));
            return new UploadPolicy.Decision.Allow();
        });

        admission.decide(ALICE, "music\\song.mp3");
        assertEquals(0L, seen.get());

        admission.served(ALICE, 4096);
        admission.decide(ALICE, "music\\song.mp3");
        assertEquals(4096L, seen.get());
    }

    @Test
    @DisplayName("the context reports privilege the way the server told us")
    void theContextReportsPrivilege() {
        AtomicReference<Boolean> seen = new AtomicReference<>();
        policy.set((request, context) -> {
            seen.set(context.requesterIsPrivileged());
            return new UploadPolicy.Decision.Allow();
        });

        admission.decide(Username.of("vip"), "music\\song.mp3");
        assertEquals(true, seen.get());

        admission.decide(ALICE, "music\\song.mp3");
        assertEquals(false, seen.get());
    }
}
