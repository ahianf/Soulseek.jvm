// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.internal.transfer.UploadScheduler;
import dev.slsk.spi.UploadContext;
import dev.slsk.spi.UploadPolicy;
import dev.slsk.spi.UploadRequest;
import dev.slsk.transfer.Priority;
import dev.slsk.transfer.RejectionReason;
import dev.slsk.transfer.TransferId;
import dev.slsk.user.UserStatistics;
import dev.slsk.user.Username;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
     * The things an admission reads, named rather than "the engine".
     *
     * <p>A test supplies them as lambdas without standing up a client. The
     * alternative — a test that reimplements the admission's rules against its
     * own fake — asserts its own copy.
     */
    private final java.util.function.Supplier<UploadPolicy> uploadPolicy;

    private final java.util.function.Supplier<Map<Integer, dev.slsk.internal.transfer.TransferInternal>> uploads;
    private final java.util.function.Predicate<String> privileged;

    /**
     * The same token source the transfer uses, so a queued request can reserve
     * the token its upload will carry on the wire.
     */
    private final java.util.function.IntSupplier tokens;

    private final dev.slsk.internal.diagnostics.DiagnosticSink diagnostic;

    /** Who is refused outright, and what they are told. */
    private final Map<Username, String> bans = new ConcurrentHashMap<>();

    /**
     * Who is waiting, in arrival order.
     *
     * <p>This used to map to the {@code position} the policy returned, which was
     * a number reported to the peer and nothing else: no code read it back as an
     * ordering, so two peers could both be told they were first. It now holds
     * the request itself, and {@link UploadScheduler} derives both the service
     * order and the reported place from it.
     */
    private final Map<PeerFile, UploadScheduler.Waiting> queued = new LinkedHashMap<>();

    /** Arrival order, so the scheduler can break ties by who asked first. */
    private final AtomicLong sequence = new AtomicLong();

    /** The embedder's ordering hints, by queued-upload id. */
    private final Map<TransferId, Priority> priorities = new ConcurrentHashMap<>();

    /**
     * Per user, when one of their uploads last started.
     *
     * <p>This is the round-robin counter. A user absent from the map has never
     * started one and therefore sorts ahead of everyone who has.
     */
    private final Map<Username, Long> lastStarted = new ConcurrentHashMap<>();

    /** Ticks each time an upload starts, so the counters above are comparable. */
    private final AtomicLong startTick = new AtomicLong();

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
            java.util.function.IntSupplier tokens,
            dev.slsk.internal.diagnostics.DiagnosticSink diagnostic) {
        this.uploadPolicy = Objects.requireNonNull(uploadPolicy, "uploadPolicy");
        this.uploads = Objects.requireNonNull(uploads, "uploads");
        this.privileged = Objects.requireNonNull(privileged, "privileged");
        this.tokens = Objects.requireNonNull(tokens, "tokens");
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

    /** Hears every refusal this admission makes; the uploads facet plugs in here. */
    @FunctionalInterface
    public interface DeniedListener {
        /** A peer was refused, by ban or by policy. */
        void denied(Username user, String path, String reason);
    }

    private volatile DeniedListener onDenied;

    /** Names where refusals are reported; UploadEvent.Denied is published from it. */
    public void onDenied(DeniedListener listener) {
        this.onDenied = java.util.Objects.requireNonNull(listener, "listener");
    }

    /**
     * Answers a peer's request.
     *
     * @param user who is asking
     * @param path the file they want
     * @return what to tell them
     */
    public UploadPolicy.Decision decide(Username user, String path) {
        UploadPolicy.Decision decision = resolve(user, path);
        if (decision instanceof UploadPolicy.Decision.Deny denied) {
            DeniedListener listener = onDenied;
            if (listener != null) {
                listener.denied(user, path, denied.message());
            }
        }
        return decision;
    }

    private UploadPolicy.Decision resolve(Username user, String path) {
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
            if (decision instanceof UploadPolicy.Decision.Queue) {
                // The policy's own position is deliberately discarded. It was
                // never an ordering — UploadPolicy.standard returns 1 for every
                // privileged user — and the peer is now told the place the
                // scheduler actually implies. What the policy decides is
                // whether to queue, not where.
                // The token is reserved here, not when a slot frees, so the
                // request and the upload it becomes are one id. They used to be
                // two — "UPLOAD:queued:7" while it waited, "UPLOAD:8301" once it
                // ran — which meant an id handed out during the wait stopped
                // resolving the moment the wait ended, and anything recording
                // uploads saw a second transfer that then disappeared.
                queued.computeIfAbsent(
                        file,
                        key -> new UploadScheduler.Waiting(
                                tokens.getAsInt(), user, path, sequence.getAndIncrement(), Priority.NORMAL));
            } else {
                // No longer waiting, either way. A caller that drew this
                // request already holds the reservation it is about to serve
                // with; a denial has nothing left to serve.
                UploadScheduler.Waiting removed = queued.remove(file);
                if (removed != null) {
                    priorities.remove(Transfers.uploadId(removed.token()));
                }
            }
        }
        return decision;
    }

    /**
     * Returns where a peer is in our queue, or {@code null} if they are not in
     * it.
     *
     * <p>The number is the place the scheduler's ordering implies, recomputed on
     * each ask rather than frozen when the request arrived: a privileged peer
     * queueing behind an ordinary one really does move ahead of them, and the
     * ordinary peer's next {@code PlaceInQueueRequest} should say so.
     *
     * @param user who is asking
     * @param path the file they asked for
     * @return their place, counting from one
     */
    public Integer place(Username user, String path) {
        return UploadScheduler.placeInQueue(schedulerState(), user, path).orElse(null);
    }

    /**
     * Returns whether this peer's request for this file is queued right now.
     *
     * @param user who asked
     * @param path the file they asked for
     * @return whether the request is waiting in the queue
     */
    public boolean isQueued(Username user, String path) {
        synchronized (lock) {
            return queued.containsKey(new PeerFile(user, path));
        }
    }

    /** Forgets a queued request, once it has been served or refused. */
    public void forget(Username user, String path) {
        synchronized (lock) {
            UploadScheduler.Waiting removed = queued.remove(new PeerFile(user, path));
            if (removed != null) {
                priorities.remove(Transfers.uploadId(removed.token()));
            }
        }
    }

    /**
     * Gives a queued upload its place in the ordering.
     *
     * <p>This is what makes {@code Uploads.prioritize} do what its javadoc has
     * always promised. The stored value used to be read back only into the
     * display snapshot, so {@code HIGH} claimed to run "ahead of NORMAL and LOW
     * work" and changed nothing.
     *
     * @param id which queued upload
     * @param priority its new priority
     * @return whether a queued upload by that id was found
     */
    public boolean prioritize(TransferId id, Priority priority) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(priority, "priority");
        synchronized (lock) {
            for (Map.Entry<PeerFile, UploadScheduler.Waiting> entry : queued.entrySet()) {
                if (Transfers.uploadId(entry.getValue().token()).equals(id)) {
                    priorities.put(id, priority);
                    entry.setValue(new UploadScheduler.Waiting(
                            entry.getValue().token(),
                            entry.getValue().user(),
                            entry.getValue().path(),
                            entry.getValue().sequence(),
                            priority));
                    return true;
                }
            }
        }
        return false;
    }

    /** Returns every queued request, in the order they will be served. */
    public List<UploadScheduler.Waiting> waiting() {
        return UploadScheduler.serviceOrder(schedulerState());
    }

    /**
     * Picks the queued upload to start next, or empty if none may start.
     *
     * <p>Slot accounting stays in the {@link UploadPolicy}, which already owns
     * it: this answers <em>who</em> is next, and the caller asks the policy
     * whether to admit them. Duplicating the slot count here would give two
     * places to change it and two chances to disagree.
     *
     * @return the next candidate
     */
    public Optional<UploadScheduler.Waiting> next() {
        return UploadScheduler.select(schedulerState());
    }

    /** Records that an upload for this user has started, for round-robin fairness. */
    public void started(Username user) {
        Objects.requireNonNull(user, "user");
        lastStarted.put(user, startTick.getAndIncrement());
    }

    private UploadScheduler.State schedulerState() {
        List<UploadScheduler.Waiting> pending;
        synchronized (lock) {
            pending = new ArrayList<>(queued.values());
        }

        Set<Username> busy = new HashSet<>();
        for (dev.slsk.internal.transfer.TransferInternal upload : uploads.get().values()) {
            dev.slsk.internal.transfer.TransferState state = upload.getState();
            if (state == null || state.contains(dev.slsk.internal.transfer.TransferState.COMPLETED)) {
                continue;
            }
            busy.add(Username.of(upload.getUsername()));
        }

        return new UploadScheduler.State(
                pending,
                busy,
                0,
                Integer.MAX_VALUE,
                candidate -> privileged.test(candidate.value()),
                Map.copyOf(lastStarted),
                UploadScheduler.Fairness.ROUND_ROBIN);
    }

    /** Gathers what the policy decides on. */
    private UploadContext context(Username user) {
        int active = 0;
        int mine = 0;
        for (dev.slsk.internal.transfer.TransferInternal upload : uploads.get().values()) {
            dev.slsk.internal.transfer.TransferState state = upload.getState();
            if (state == null || state.contains(dev.slsk.internal.transfer.TransferState.COMPLETED)) {
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
