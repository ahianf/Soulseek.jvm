// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.transfer.TransferDirection;
import dev.slsk.internal.transfer.TransferInternal;
import dev.slsk.spi.UploadPolicy;
import dev.slsk.transfer.Priority;
import dev.slsk.transfer.RejectionReason;
import dev.slsk.user.Username;
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

    /** Stands in for the client's token factory: a counter, as that one is. */
    private final java.util.concurrent.atomic.AtomicInteger nextToken =
            new java.util.concurrent.atomic.AtomicInteger(700);

    private final UploadAdmission admission = new UploadAdmission(
            policy::get,
            uploads::get,
            username -> "vip".equals(username),
            nextToken::getAndIncrement);

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
        // The policy's own number is deliberately not echoed back. It never
        // was an ordering — UploadPolicy.standard hands every privileged user a
        // 1 — so the peer is told the place the scheduler's ordering implies.
        // One queued request means one place: first.
        policy.set((request, context) -> new UploadPolicy.Decision.Queue(7));
        admission.decide(ALICE, "music\\song.mp3");
        assertEquals(1, admission.place(ALICE, "music\\song.mp3"));

        assertNull(admission.place(ALICE, "music\\other.mp3"), "a file we are not holding has no place");
        assertNull(admission.place(BOB, "music\\song.mp3"), "and neither does another peer");

        // Allowing them takes them out of the queue: they are being served.
        policy.set((request, context) -> new UploadPolicy.Decision.Allow());
        admission.decide(ALICE, "music\\song.mp3");
        assertNull(admission.place(ALICE, "music\\song.mp3"));
    }

    /**
     * The queue and the transfer it becomes are one id, not two.
     *
     * <p>A queued request used to be called {@code UPLOAD:queued:7} and the
     * upload that served it {@code UPLOAD:8301}. Nothing joined them: an id
     * handed to a consumer while the request waited stopped resolving the
     * moment a slot freed, {@code prioritize} could only ever name the half
     * that was about to be discarded, and a consumer recording what it saw
     * recorded two transfers per request — the waiting one, which vanished
     * without an outcome, and the running one.
     */
    @Test
    @DisplayName("a queued request wears the id its upload will wear")
    void aQueuedRequestReservesTheTokenItsUploadWillCarry() {
        policy.set((request, context) -> new UploadPolicy.Decision.Queue(1));
        admission.decide(ALICE, "music\\song.mp3");

        int token = admission.waiting().get(0).token();
        assertEquals(700, token, "reserved from the token factory, not invented");

        // The id the facet reports for the wait is the one Transfers derives
        // from the transfer once it is running under that token.
        TransferInternal running =
                new TransferInternal(TransferDirection.UPLOAD, ALICE.value(), "music\\song.mp3", token);
        assertEquals(Transfers.id(running.toTransfer()), Transfers.uploadId(token));

        // And it is the id prioritize names, while the request is still waiting.
        assertTrue(admission.prioritize(Transfers.uploadId(token), Priority.HIGH));
        assertEquals(Priority.HIGH, admission.waiting().get(0).priority());
    }

    @Test
    void eachQueuedRequestReservesItsOwnToken() {
        policy.set((request, context) -> new UploadPolicy.Decision.Queue(1));
        admission.decide(ALICE, "music\\song.mp3");
        admission.decide(BOB, "music\\song.mp3");
        // Asking twice for the same file does not reserve a second token: the
        // request is already waiting, and it is one request.
        admission.decide(ALICE, "music\\song.mp3");

        assertEquals(
                java.util.Set.of(700, 701),
                admission.waiting().stream()
                        .map(dev.slsk.internal.transfer.UploadScheduler.Waiting::token)
                        .collect(java.util.stream.Collectors.toSet()));
    }

    @Test
    void forgetDropsAQueuedRequest() {
        policy.set((request, context) -> new UploadPolicy.Decision.Queue(1));
        admission.decide(ALICE, "music\\song.mp3");
        admission.forget(ALICE, "music\\song.mp3");
        assertNull(admission.place(ALICE, "music\\song.mp3"));
    }

    @Test
    @DisplayName("the queue is keyed by user and path, not by the two joined together")
    void theQueueKeyIsTyped() {
        // Joining a username and a path into one string needs a separator no
        // username and no path can contain, which is why the key used to be a
        // raw NUL byte written into the source. A pair that would collide under
        // any printable separator is the cheapest way to say the key is a pair.
        policy.set((request, context) -> new UploadPolicy.Decision.Queue(3));
        admission.decide(Username.of("alice bob"), "music\\song.mp3");

        // The place is the scheduler's, so a single queued request is first.
        // What this test is about is the key: the lookup below must miss.
        assertEquals(1, admission.place(Username.of("alice bob"), "music\\song.mp3"));
        assertNull(admission.place(ALICE, "bob music\\song.mp3"));
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
