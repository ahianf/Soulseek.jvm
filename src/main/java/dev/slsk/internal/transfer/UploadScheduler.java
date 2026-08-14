// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-FileCopyrightText: Nicotine+ Contributors
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

import dev.slsk.transfer.Priority;
import dev.slsk.user.Username;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Which queued upload starts next when a slot frees.
 *
 * <p>The library had admission and no selection. {@code UploadAdmission} decided
 * whether a peer's request was allowed, queued or denied, and a queued request
 * then sat there: nothing ever drew one when a slot freed, and the
 * {@code position} in {@code Decision.Queue} was a number reported to the peer
 * rather than an ordering anything consulted. This supplies the missing half.
 *
 * <p>Selection is a pure function of the state handed to it — {@link #select} is
 * static and reads nothing else. That is what makes Nicotine+'s scenario table
 * portable: the same queued/active/expected triples can be replayed against it
 * without standing up a client, a connection or a policy.
 *
 * <h2>Order</h2>
 *
 * <pre>
 * 1. server-privileged        (protocol-mandated, hard gate)
 * 2. Priority.HIGH            (embedder-set — where buddies land)
 * 3. Priority.NORMAL          (default)
 * 4. Priority.LOW             (only when nothing above wants the slot)
 *         within each tier: round-robin across users, FIFO within a user
 * </pre>
 *
 * <p><strong>Privilege is a gate, not a weight.</strong> If any user in a higher
 * tier has queued work, every user below it is skipped entirely — Nicotine+
 * behaves the same way, and a weighting would let a large enough backlog of
 * ordinary users starve a privileged one.
 *
 * <h2>Divergence from Nicotine+</h2>
 *
 * <p>Nicotine+ computes {@code is_privileged(u)} as "on the server's privileged
 * list <em>or</em> buddy-prioritised", where buddy-prioritisation reads two
 * config flags ({@code preferfriends}, per-buddy {@code is_prioritized}). This
 * collapses that second half into {@link Priority#HIGH}, because who counts as a
 * buddy is the embedding application's knowledge and not the library's. The
 * library keeps only the half the protocol mandates — the server's privileged
 * list — and the embedder expresses the rest through the {@code Uploads}
 * facet's existing {@code prioritize} call. No buddy list is added here, and
 * none should be.
 *
 * <p>A consequence worth stating plainly: this makes four tiers where Nicotine+
 * has two, so a HIGH-priority non-privileged user outranks a NORMAL one, which
 * has no Nicotine+ equivalent.
 */
public final class UploadScheduler {

    private UploadScheduler() {}

    /** How users are picked within a tier. */
    public enum Fairness {

        /**
         * The user who has waited longest since one of their uploads last
         * started. The default, and Nicotine+'s.
         */
        ROUND_ROBIN,

        /**
         * The earliest queued request whose user is free, regardless of who
         * that user is. Nicotine+'s {@code fifoqueue} setting.
         *
         * <p>Nothing exported selects this: it would need either a new option
         * on a frozen surface or a {@code UploadPolicy} extension, and
         * {@code CROSS_CHECK_PLAN.md} §B3 says the freeze wins. It is
         * implemented and tested so the ported Nicotine+ FIFO scenarios pin the
         * algorithm, and so that wiring it to a setting later is a one-line
         * change rather than a rewrite.
         */
        FIFO
    }

    /**
     * A request waiting for a slot.
     *
     * @param token the upload token reserved for it. The upload keeps it when a
     *     slot frees, so the id the uploads facet reports for this request is
     *     the id of the transfer it becomes — what {@code prioritize} names
     *     while it waits still names it once it runs
     * @param user who asked
     * @param path what they asked for
     * @param sequence arrival order; lower is earlier
     * @param priority the embedder's ordering hint
     */
    public record Waiting(int token, Username user, String path, long sequence, Priority priority) {

        /** Validates and returns the request. */
        public Waiting {
            Objects.requireNonNull(user, "user");
            Objects.requireNonNull(path, "path");
            Objects.requireNonNull(priority, "priority");
        }
    }

    /**
     * Everything selection reads.
     *
     * @param waiting the queued requests, in any order — {@code sequence} carries arrival
     * @param usersWithActiveUpload who already has an upload running
     * @param activeSlots how many uploads are running
     * @param maxSlots how many may run at once
     * @param serverPrivileged the server's privileged list, from server code 69
     * @param lastStarted per user, the sequence number at which one of their
     *     uploads last started; a user absent from the map has never started one
     * @param fairness how to pick within a tier
     */
    public record State(
            List<Waiting> waiting,
            Set<Username> usersWithActiveUpload,
            int activeSlots,
            int maxSlots,
            Predicate<Username> serverPrivileged,
            Map<Username, Long> lastStarted,
            Fairness fairness) {

        /** Validates and returns the state. */
        public State {
            Objects.requireNonNull(waiting, "waiting");
            Objects.requireNonNull(usersWithActiveUpload, "usersWithActiveUpload");
            Objects.requireNonNull(serverPrivileged, "serverPrivileged");
            Objects.requireNonNull(lastStarted, "lastStarted");
            Objects.requireNonNull(fairness, "fairness");
            waiting = List.copyOf(waiting);
            usersWithActiveUpload = Set.copyOf(usersWithActiveUpload);
            lastStarted = Map.copyOf(lastStarted);
        }
    }

    /**
     * Picks the upload to start next.
     *
     * @param state what is true right now
     * @return the request to start, or empty if none may
     */
    public static Optional<Waiting> select(State state) {
        Objects.requireNonNull(state, "state");
        if (state.activeSlots() >= state.maxSlots()) {
            return Optional.empty();
        }

        // One upload per user at a time. Nicotine+ enforces the same rule, and
        // without it a single user with a deep queue takes every slot.
        List<Waiting> eligible = state.waiting().stream()
                .filter(candidate -> !state.usersWithActiveUpload().contains(candidate.user()))
                .toList();
        if (eligible.isEmpty()) {
            return Optional.empty();
        }

        int bestTier = eligible.stream()
                .mapToInt(candidate -> tierOf(candidate, state.serverPrivileged()))
                .min()
                .orElseThrow();
        List<Waiting> tier = eligible.stream()
                .filter(candidate -> tierOf(candidate, state.serverPrivileged()) == bestTier)
                .toList();

        if (state.fairness() == Fairness.FIFO) {
            return tier.stream().min(Comparator.comparingLong(Waiting::sequence));
        }

        // Round-robin: the user who has waited longest since one of their
        // uploads last started. A user who has never started one has waited
        // longest of all, so they sort ahead of everyone; ties fall back to
        // arrival order, which is what makes the first pass follow the queue.
        Map<Username, Long> earliestSequence = new HashMap<>();
        for (Waiting candidate : tier) {
            earliestSequence.merge(candidate.user(), candidate.sequence(), Math::min);
        }

        Username chosen = null;
        long chosenLastStarted = 0;
        long chosenSequence = 0;
        for (Map.Entry<Username, Long> entry : earliestSequence.entrySet()) {
            long lastStarted = state.lastStarted().getOrDefault(entry.getKey(), Long.MIN_VALUE);
            long sequence = entry.getValue();
            if (chosen == null
                    || lastStarted < chosenLastStarted
                    || (lastStarted == chosenLastStarted && sequence < chosenSequence)) {
                chosen = entry.getKey();
                chosenLastStarted = lastStarted;
                chosenSequence = sequence;
            }
        }

        Username winner = chosen;
        // FIFO within the chosen user: a peer's own requests keep their order.
        return tier.stream()
                .filter(candidate -> candidate.user().equals(winner))
                .min(Comparator.comparingLong(Waiting::sequence));
    }

    /**
     * Returns the queue in the order it will actually be served.
     *
     * <p>This is what makes {@code PlaceInQueueResponse} honest. Before this,
     * the position a peer was told came from the policy's
     * {@code Decision.Queue(position)} — {@code UploadPolicy.standard} returned
     * {@code 1} for privileged users and the queue depth for everyone else, so
     * two peers could be told they were both first and neither number described
     * when they would be served.
     *
     * <p>The ordering is the repeated application of {@link #select} with each
     * winner removed, which is the only definition that cannot drift from it.
     *
     * @param state what is true right now
     * @return every waiting request, in service order
     */
    public static List<Waiting> serviceOrder(State state) {
        Objects.requireNonNull(state, "state");
        List<Waiting> remaining = new ArrayList<>(state.waiting());
        Map<Username, Long> lastStarted = new HashMap<>(state.lastStarted());
        List<Waiting> order = new ArrayList<>(remaining.size());

        // Projecting the order means asking "who is next" repeatedly, so the
        // slot and active-upload gates are lifted: they say when the queue
        // moves, not what its order is. A peer waiting behind a running upload
        // still has a place, and that place is what it is asking for.
        long tick = 0;
        while (!remaining.isEmpty()) {
            Optional<Waiting> next = select(new State(
                    remaining,
                    Set.of(),
                    0,
                    Integer.MAX_VALUE,
                    state.serverPrivileged(),
                    lastStarted,
                    state.fairness()));
            if (next.isEmpty()) {
                break;
            }
            Waiting winner = next.get();
            order.add(winner);
            remaining.remove(winner);
            lastStarted.put(winner.user(), tick++);
        }
        return List.copyOf(order);
    }

    /**
     * Returns a peer's place in the queue, counting from one.
     *
     * @param state what is true right now
     * @param user who is asking
     * @param path the file they asked about
     * @return their place, or empty if they are not waiting for it
     */
    public static Optional<Integer> placeInQueue(State state, Username user, String path) {
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(path, "path");
        List<Waiting> order = serviceOrder(state);
        for (int index = 0; index < order.size(); index++) {
            Waiting candidate = order.get(index);
            if (candidate.user().equals(user) && candidate.path().equals(path)) {
                return Optional.of(index + 1);
            }
        }
        return Optional.empty();
    }

    private static int tierOf(Waiting candidate, Predicate<Username> serverPrivileged) {
        if (serverPrivileged.test(candidate.user())) {
            return 0;
        }
        return switch (candidate.priority()) {
            case HIGH -> 1;
            case NORMAL -> 2;
            case LOW -> 3;
        };
    }
}
