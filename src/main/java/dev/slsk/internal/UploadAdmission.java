// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.RejectionReason;
import dev.slsk.UserStatistics;
import dev.slsk.Username;
import dev.slsk.spi.UploadContext;
import dev.slsk.spi.UploadPolicy;
import dev.slsk.spi.UploadRequest;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What happens when a peer asks for a file.
 *
 * <p>The policy decides; this gathers what it decides from, keeps the queue the
 * decision refers to, and enforces the one rule no policy is allowed to
 * override — a banned user is refused before a policy is asked, because a ban is
 * not a preference the application should be able to lose track of.
 *
 * <p>Bans are checked here rather than inside {@link UploadPolicy#standard} so
 * that a consumer replacing the standard policy cannot accidentally unban
 * everyone. That is the difference between a default and an invariant.
 */
public final class UploadAdmission {

    /**
     * The four things an admission reads, named rather than "the engine".
     *
     * <p>A test supplies them as four lambdas without standing up a client. The
     * alternative — a test that reimplements the admission's rules against its
     * own fake — asserts its own copy.
     */
    private final java.util.function.Supplier<UploadPolicy> uploadPolicy;

    private final java.util.function.Supplier<Map<Integer, dev.slsk.internal.transfer.TransferInternal>> uploads;
    private final java.util.function.Predicate<String> privileged;
    private final dev.slsk.internal.diagnostics.DiagnosticSink diagnostic;

    /** Who is refused outright, and what they are told. */
    private final Map<Username, String> bans = new ConcurrentHashMap<>();

    /** Who is waiting, in the order the policy put them. */
    private final Map<PeerFile, Integer> queued = new LinkedHashMap<>();

    /** How much we have sent each peer this session. */
    private final Map<Username, AtomicLong> sent = new ConcurrentHashMap<>();

    private final Object lock = new Object();

    /**
     * Creates an admission.
     *
     * @param client what it reads its context from
     */
    public UploadAdmission(
            java.util.function.Supplier<UploadPolicy> uploadPolicy,
            java.util.function.Supplier<Map<Integer, dev.slsk.internal.transfer.TransferInternal>> uploads,
            java.util.function.Predicate<String> privileged,
            dev.slsk.internal.diagnostics.DiagnosticSink diagnostic) {
        this.uploadPolicy = Objects.requireNonNull(uploadPolicy, "uploadPolicy");
        this.uploads = Objects.requireNonNull(uploads, "uploads");
        this.privileged = Objects.requireNonNull(privileged, "privileged");
        this.diagnostic = Objects.requireNonNull(diagnostic, "diagnostic");
    }

    /** Refuses a user until they are unbanned. Idempotent. */
    public void ban(Username user, String reason) {
        bans.put(Objects.requireNonNull(user, "user"), reason == null ? "Banned." : reason);
    }

    /** Stops refusing a user. Idempotent. */
    public void unban(Username user) {
        bans.remove(Objects.requireNonNull(user, "user"));
    }

    /** Returns who is banned, and why. */
    public Map<Username, String> banned() {
        return Map.copyOf(bans);
    }

    /** Records bytes served, for a policy that decides on what it has given. */
    public void served(Username user, long bytes) {
        sent.computeIfAbsent(user, key -> new AtomicLong()).addAndGet(bytes);
    }

    /**
     * Answers a peer's request.
     *
     * @param user who is asking
     * @param path the file they want
     * @return what to tell them
     */
    public UploadPolicy.Decision decide(Username user, String path) {
        String reason = bans.get(user);
        if (reason != null) {
            // Ahead of the policy, and not expressible by one: a consumer that
            // replaces the standard policy must not be able to unban a user by
            // forgetting to check.
            return new UploadPolicy.Decision.Deny(RejectionReason.BANNED, reason);
        }

        UploadPolicy.Decision decision;
        try {
            decision = uploadPolicy.get().decide(new UploadRequest(user, path, 0), context(user));
        } catch (RuntimeException failure) {
            diagnostic.warning("The upload policy threw; refusing the request", failure);
            return new UploadPolicy.Decision.Deny(RejectionReason.UNKNOWN, "Upload policy failed.");
        }
        if (decision == null) {
            return new UploadPolicy.Decision.Deny(RejectionReason.UNKNOWN, "Upload policy returned nothing.");
        }

        synchronized (lock) {
            PeerFile file = new PeerFile(user, path);
            if (decision instanceof UploadPolicy.Decision.Queue queue) {
                queued.put(file, queue.position());
            } else {
                queued.remove(file);
            }
        }
        return decision;
    }

    /**
     * Returns where a peer is in our queue, or {@code null} if they are not in
     * it.
     *
     * @param user who is asking
     * @param path the file they asked for
     * @return their place, counting from one
     */
    public Integer place(Username user, String path) {
        synchronized (lock) {
            return queued.get(new PeerFile(user, path));
        }
    }

    /** Forgets a queued request, once it has been served or refused. */
    public void forget(Username user, String path) {
        synchronized (lock) {
            queued.remove(new PeerFile(user, path));
        }
    }

    /** Gathers what the policy decides on. */
    private UploadContext context(Username user) {
        int active = 0;
        int mine = 0;
        for (dev.slsk.internal.transfer.TransferInternal upload : uploads.get().values()) {
            dev.slsk.internal.TransferState state = upload.getState();
            if (state == null || state.contains(dev.slsk.internal.TransferState.COMPLETED)) {
                continue;
            }
            active++;
            if (user.value().equals(upload.getUsername())) {
                mine++;
            }
        }
        int depth;
        synchronized (lock) {
            depth = queued.size();
        }

        int activeSlots = active;
        int activeForRequester = mine;
        int queueDepth = depth;
        return new UploadContext() {
            @Override
            public UserStatistics requesterStatistics() {
                // The server tells us this unprompted for users we watch; for
                // anyone else an empty record is honest, and a policy deciding
                // on figures it does not have is deciding on a guess.
                return new UserStatistics(user, 0, 0, 0, 0);
            }

            @Override
            public boolean requesterIsPrivileged() {
                return privileged.test(user.value());
            }

            @Override
            public long bytesAlreadySentTo(Username who) {
                AtomicLong total = sent.get(who);
                return total == null ? 0 : total.get();
            }

            @Override
            public int activeSlots() {
                return activeSlots;
            }

            @Override
            public int queueDepth() {
                return queueDepth;
            }

            @Override
            public int activeSlotsForRequester() {
                return activeForRequester;
            }
        };
    }
}
