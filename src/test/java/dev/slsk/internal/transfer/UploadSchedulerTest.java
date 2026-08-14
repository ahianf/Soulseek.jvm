// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-FileCopyrightText: Nicotine+ Contributors
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.transfer.UploadScheduler.Fairness;
import dev.slsk.internal.transfer.UploadScheduler.State;
import dev.slsk.internal.transfer.UploadScheduler.Waiting;
import dev.slsk.transfer.Priority;
import dev.slsk.user.Username;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class UploadSchedulerTest {

    private static final Set<String> PRIVILEGED = Set.of("puser1", "puser2");

    /**
     * Replays Nicotine+'s {@code consume_transfers} loop.
     *
     * <p>Their loop calls the selector until nothing is left, finishing one
     * in-progress upload each pass and recording {@code None} whenever every
     * queued user already has an upload running. {@code clearFirst} chooses
     * whether that finish happens before or after the pick, which is what
     * separates their contention cases from their no-contention ones.
     *
     * @return the username picked on each pass, with {@code null} for a pass
     *     that could pick nobody
     */
    private static List<String> consume(
            List<String> queued, List<String> inProgress, Fairness fairness, boolean clearFirst) {
        List<Waiting> waiting = new ArrayList<>();
        long sequence = 0;
        for (String user : queued) {
            waiting.add(
                    new Waiting((int) sequence, Username.of(user), user + "/" + sequence, sequence, Priority.NORMAL));
            sequence++;
        }

        List<Username> active =
                new ArrayList<>(inProgress.stream().map(Username::of).toList());
        Map<Username, Long> lastStarted = new HashMap<>();
        long tick = 0;
        // Nicotine+ activates its pre-existing in-progress transfers first, so
        // their counters are already set when the first pick happens.
        for (Username user : active) {
            lastStarted.put(user, tick++);
        }

        List<String> picked = new ArrayList<>();
        int emptyPasses = 0;
        while (!(waiting.isEmpty() && active.isEmpty()) && emptyPasses < 2) {
            if (clearFirst && !active.isEmpty()) {
                active.remove(0);
            }

            Optional<Waiting> candidate = UploadScheduler.select(new State(
                    waiting,
                    new HashSet<>(active),
                    0,
                    Integer.MAX_VALUE,
                    user -> PRIVILEGED.contains(user.value()),
                    lastStarted,
                    fairness));

            if (!clearFirst && !active.isEmpty()) {
                active.remove(0);
            }

            if (candidate.isEmpty()) {
                emptyPasses++;
                picked.add(null);
                continue;
            }

            emptyPasses = 0;
            Waiting winner = candidate.get();
            waiting.remove(winner);
            picked.add(winner.user().value());
            active.add(winner.user());
            lastStarted.put(winner.user(), tick++);
        }
        return picked;
    }

    private static List<String> roundRobin(List<String> queued, List<String> inProgress, boolean clearFirst) {
        return consume(queued, inProgress, Fairness.ROUND_ROBIN, clearFirst);
    }

    private static List<String> fifo(List<String> queued, List<String> inProgress, boolean clearFirst) {
        return consume(queued, inProgress, Fairness.FIFO, clearFirst);
    }

    @Nested
    @DisplayName("Nicotine+ round-robin scenarios")
    class RoundRobin {

        @Test
        @DisplayName("test_round_robin_basic")
        void basic() {
            assertEquals(
                    List.of("user1", "user2", "user3", "user1", "user2", "user3"),
                    withoutTrailingNull(roundRobin(
                            List.of("user1", "user1", "user2", "user2", "user3", "user3"), List.of(), false)));
        }

        @Test
        @DisplayName("test_round_robin_no_contention")
        void noContention() {
            assertEquals(
                    List.of("user1", "user2", "user3", "user1", "user2", "user3"),
                    withoutTrailingNull(roundRobin(
                            List.of("user1", "user1", "user2", "user2", "user3", "user3"), List.of(), true)));
        }

        @Test
        @DisplayName("test_round_robin_one_user")
        void oneUser() {
            // A single user cannot hold two slots, so every other pass picks
            // nobody even though work is queued.
            assertEquals(
                    java.util.Arrays.asList("user1", null, "user1", null),
                    roundRobin(List.of("user1", "user1"), List.of(), false));
        }

        @Test
        @DisplayName("test_round_robin_returning_user")
        void returningUser() {
            assertEquals(
                    List.of("user1", "user2", "user3", "user1", "user2", "user3", "user1", "user2", "user3", "user1"),
                    withoutTrailingNull(roundRobin(
                            List.of(
                                    "user1", "user1", "user2", "user2", "user2", "user3", "user3", "user3", "user1",
                                    "user1"),
                            List.of(),
                            false)));
        }

        @Test
        @DisplayName("test_round_robin_in_progress")
        void inProgress() {
            assertEquals(
                    List.of("user2", "user1", "user2", "user1"),
                    withoutTrailingNull(
                            roundRobin(List.of("user1", "user1", "user2", "user2"), List.of("user1"), false)));
        }

        @Test
        @DisplayName("test_round_robin_privileged")
        void privileged() {
            // Privilege is a gate: both privileged users drain before either
            // ordinary user is considered, even though user1 and user2 queued
            // first.
            assertEquals(
                    List.of("puser1", "puser2", "puser1", "user1", "user2"),
                    withoutTrailingNull(
                            roundRobin(List.of("user1", "user2", "puser1", "puser1", "puser2"), List.of(), false)));
        }
    }

    @Nested
    @DisplayName("Nicotine+ FIFO scenarios")
    class Fifo {

        @Test
        @DisplayName("test_fifo_basic")
        void basic() {
            assertEquals(
                    java.util.Arrays.asList("user1", "user2", "user1", "user2", "user3", null, "user3", null),
                    fifo(List.of("user1", "user1", "user2", "user2", "user3", "user3"), List.of(), false));
        }

        @Test
        @DisplayName("test_fifo_robin_no_contention")
        void noContention() {
            assertEquals(
                    List.of("user1", "user1", "user2", "user2", "user3", "user3"),
                    withoutTrailingNull(
                            fifo(List.of("user1", "user1", "user2", "user2", "user3", "user3"), List.of(), true)));
        }

        @Test
        @DisplayName("test_fifo_one_user")
        void oneUser() {
            assertEquals(
                    java.util.Arrays.asList("user1", null, "user1", null),
                    fifo(List.of("user1", "user1"), List.of(), false));
        }

        @Test
        @DisplayName("test_fifo_returning_user")
        void returningUser() {
            assertEquals(
                    List.of("user1", "user2", "user1", "user2", "user3", "user2", "user3", "user1", "user3", "user1"),
                    withoutTrailingNull(fifo(
                            List.of(
                                    "user1", "user1", "user2", "user2", "user2", "user3", "user3", "user3", "user1",
                                    "user1"),
                            List.of(),
                            false)));
        }

        @Test
        @DisplayName("test_fifo_in_progress")
        void inProgress() {
            assertEquals(
                    List.of("user2", "user1", "user2", "user1"),
                    withoutTrailingNull(fifo(List.of("user1", "user1", "user2", "user2"), List.of("user1"), false)));
        }

        @Test
        @DisplayName("test_fifo_privileged")
        void privileged() {
            assertEquals(
                    List.of("puser1", "puser2", "puser1", "user1", "user2"),
                    withoutTrailingNull(
                            fifo(List.of("user1", "user2", "puser1", "puser1", "puser2"), List.of(), false)));
        }
    }

    @Nested
    @DisplayName("Priority tiering — the divergence from Nicotine+")
    class Tiering {

        @Test
        @DisplayName("HIGH runs ahead of NORMAL, which runs ahead of LOW")
        void priorityOrdersTiers() {
            List<Waiting> waiting = List.of(
                    waiting(0, "low", Priority.LOW),
                    waiting(1, "normal", Priority.NORMAL),
                    waiting(2, "high", Priority.HIGH));

            assertEquals(
                    List.of("high", "normal", "low"),
                    UploadScheduler.serviceOrder(state(waiting, Set.of())).stream()
                            .map(candidate -> candidate.user().value())
                            .toList());
        }

        @Test
        @DisplayName("server-privileged outranks even HIGH")
        void privilegeOutranksPriority() {
            List<Waiting> waiting = List.of(waiting(0, "high", Priority.HIGH), waiting(1, "puser1", Priority.LOW));

            assertEquals(
                    "puser1",
                    UploadScheduler.select(state(waiting, Set.of()))
                            .orElseThrow()
                            .user()
                            .value());
        }

        @Test
        @DisplayName("a tier is a gate, so a deep NORMAL queue never starves a HIGH user")
        void tierIsAGateNotAWeight() {
            List<Waiting> waiting = new ArrayList<>();
            for (int index = 0; index < 50; index++) {
                waiting.add(waiting(index, "normal" + index, Priority.NORMAL));
            }
            waiting.add(waiting(100, "high", Priority.HIGH));

            assertEquals(
                    "high",
                    UploadScheduler.select(state(waiting, Set.of()))
                            .orElseThrow()
                            .user()
                            .value());
        }

        @Test
        @DisplayName("within a tier, one user's own requests keep their order")
        void fifoWithinAUser() {
            List<Waiting> waiting = List.of(waiting(5, "user1", Priority.NORMAL), waiting(1, "user1", Priority.NORMAL));

            assertEquals(
                    1L,
                    UploadScheduler.select(state(waiting, Set.of()))
                            .orElseThrow()
                            .sequence());
        }
    }

    @Nested
    @DisplayName("Slots and places")
    class SlotsAndPlaces {

        @Test
        @DisplayName("no candidate is drawn when every slot is busy")
        void respectsSlotLimit() {
            List<Waiting> waiting = List.of(waiting(0, "user1", Priority.NORMAL));
            State full = new State(waiting, Set.of(), 2, 2, user -> false, Map.of(), Fairness.ROUND_ROBIN);

            assertTrue(UploadScheduler.select(full).isEmpty());
        }

        @Test
        @DisplayName("a user already uploading is not drawn again")
        void oneUploadPerUser() {
            List<Waiting> waiting = List.of(waiting(0, "user1", Priority.NORMAL));

            assertTrue(UploadScheduler.select(state(waiting, Set.of(Username.of("user1"))))
                    .isEmpty());
        }

        @Test
        @DisplayName("place in queue is the position the ordering actually implies")
        void placeFollowsTheOrdering() {
            List<Waiting> waiting = List.of(
                    waiting(0, "user1", Priority.NORMAL),
                    waiting(1, "user2", Priority.NORMAL),
                    waiting(2, "puser1", Priority.NORMAL));
            State state = state(waiting, Set.of());

            // The privileged peer queued last and is served first, so the two
            // ordinary peers are told 2 and 3 — not "1" and "queue depth", the
            // two unrelated numbers the policy used to report.
            assertEquals(
                    1,
                    UploadScheduler.placeInQueue(state, Username.of("puser1"), "puser1/2")
                            .orElseThrow());
            assertEquals(
                    2,
                    UploadScheduler.placeInQueue(state, Username.of("user1"), "user1/0")
                            .orElseThrow());
            assertEquals(
                    3,
                    UploadScheduler.placeInQueue(state, Username.of("user2"), "user2/1")
                            .orElseThrow());
        }

        @Test
        @DisplayName("a peer not waiting for that file has no place")
        void unknownRequestHasNoPlace() {
            State state = state(List.of(waiting(0, "user1", Priority.NORMAL)), Set.of());

            assertTrue(UploadScheduler.placeInQueue(state, Username.of("user1"), "other")
                    .isEmpty());
            assertTrue(UploadScheduler.placeInQueue(state, Username.of("nobody"), "user1/0")
                    .isEmpty());
        }

        @Test
        @DisplayName("service order covers every waiting request exactly once")
        void serviceOrderIsATotalOrdering() {
            List<Waiting> waiting = List.of(
                    waiting(0, "user1", Priority.LOW),
                    waiting(1, "user1", Priority.NORMAL),
                    waiting(2, "puser1", Priority.HIGH),
                    waiting(3, "user2", Priority.NORMAL));

            List<Waiting> order = UploadScheduler.serviceOrder(state(waiting, Set.of()));

            assertEquals(waiting.size(), order.size());
            assertEquals(Set.copyOf(waiting), Set.copyOf(order));
        }
    }

    private static Waiting waiting(long sequence, String user, Priority priority) {
        return new Waiting((int) sequence, Username.of(user), user + "/" + sequence, sequence, priority);
    }

    private static State state(List<Waiting> waiting, Set<Username> active) {
        return new State(
                waiting,
                active,
                active.size(),
                Integer.MAX_VALUE,
                user -> PRIVILEGED.contains(user.value()),
                Map.of(),
                Fairness.ROUND_ROBIN);
    }

    /**
     * Drops the trailing {@code null} Nicotine+'s harness records once the
     * queue is drained but an upload is still finishing. It marks the end of
     * their loop rather than a scheduling decision.
     */
    private static List<String> withoutTrailingNull(List<String> picked) {
        List<String> trimmed = new ArrayList<>(picked);
        while (!trimmed.isEmpty() && trimmed.get(trimmed.size() - 1) == null) {
            trimmed.remove(trimmed.size() - 1);
        }
        return trimmed;
    }
}
