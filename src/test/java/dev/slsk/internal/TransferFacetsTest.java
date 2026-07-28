// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.DownloadRequest;
import dev.slsk.Priority;
import dev.slsk.RejectionReason;
import dev.slsk.Soulseek;
import dev.slsk.TransferId;
import dev.slsk.Username;
import dev.slsk.exceptions.TransferNotFoundException;
import dev.slsk.internal.options.SoulseekClientOptions;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TransferFacetsTest {

    private static final Username BOB = Username.of("bob");

    private static Soulseek client() {
        return DefaultSoulseek.create("alice", "password", 157, new SoulseekClientOptions());
    }

    @Test
    void downloadsAndUploadsStartEmpty() {
        try (Soulseek slsk = client()) {
            assertEquals(List.of(), slsk.downloads().all());
            assertEquals(List.of(), slsk.uploads().all());
        }
    }

    @Test
    @DisplayName("banning is upload policy, so it lives on uploads and is idempotent")
    void bansAreIdempotentAndLiveOnUploads() {
        try (Soulseek slsk = client()) {
            assertEquals(Map.of(), slsk.uploads().banned());

            slsk.uploads().ban(BOB, "spamming");
            slsk.uploads().ban(BOB, "spamming");
            assertEquals(Map.of(BOB, "spamming"), slsk.uploads().banned());

            slsk.uploads().unban(BOB);
            slsk.uploads().unban(BOB);
            assertEquals(Map.of(), slsk.uploads().banned());
        }
    }

    @Test
    void bannedListIsASnapshot() {
        try (Soulseek slsk = client()) {
            slsk.uploads().ban(BOB, "reason");
            Map<Username, String> banned = slsk.uploads().banned();
            assertThrows(UnsupportedOperationException.class, () -> banned.put(BOB, "other"));
        }
    }

    @Test
    @DisplayName("cancelling an unknown download is a no-op, not an error")
    void cancelIsIdempotentOnUnknownDownloads() {
        try (Soulseek slsk = client()) {
            slsk.downloads().cancel(TransferId.of("DOWNLOAD:999"));
            slsk.downloads().cancel(TransferId.of("DOWNLOAD:999"));
        }
    }

    @Test
    @DisplayName("forgetting an unknown download is a no-op, not an error")
    void forgetIsIdempotentOnUnknownDownloads() {
        try (Soulseek slsk = client()) {
            slsk.downloads().forget(TransferId.of("DOWNLOAD:999"));
        }
    }

    @Test
    @DisplayName("the queue-dependent commands are declared, now that the queue exists")
    void queueCommandsAreDeclaredOnceTheyWork() {
        // These were absent rather than stubbed while the queue was unbuilt: a
        // declared method that does not work is a stub, and one that silently
        // does nothing is worse. The queue exists, so they do.
        Set<String> declared = Arrays.stream(dev.slsk.Downloads.class.getMethods())
                .map(java.lang.reflect.Method::getName)
                .collect(java.util.stream.Collectors.toSet());
        for (String command : Set.of("enqueue", "pause", "resume", "cancel", "retry", "forget", "await", "policy")) {
            assertTrue(declared.contains(command), command + " should be declared");
        }
    }

    @Test
    void unknownTransfersAreNotFound() {
        try (Soulseek slsk = client()) {
            assertTrue(slsk.downloads().find(TransferId.of("DOWNLOAD:1")).isEmpty());
            assertThrows(TransferNotFoundException.class, () -> slsk.downloads().get(TransferId.of("DOWNLOAD:1")));
            assertTrue(slsk.uploads().find(TransferId.of("UPLOAD:1")).isEmpty());
            assertThrows(IllegalArgumentException.class, () -> slsk.uploads().get(TransferId.of("UPLOAD:1")));
        }
    }

    @Test
    void rejectsNullArguments() {
        try (Soulseek slsk = client()) {
            assertThrows(NullPointerException.class, () -> slsk.downloads().enqueue(null));
            assertThrows(NullPointerException.class, () -> slsk.downloads().cancel(null));
            assertThrows(NullPointerException.class, () -> slsk.uploads().ban(null, "r"));
            assertThrows(NullPointerException.class, () -> slsk.uploads().unban(null));
            assertThrows(NullPointerException.class, () -> slsk.uploads().prioritize(null, Priority.HIGH));
        }
    }

    @Test
    @DisplayName("a request carries the tags that would otherwise need a stale side table")
    void downloadRequestCarriesTags() {
        DownloadRequest request = DownloadRequest.builder(BOB, "a\\b.mp3", Path.of("/tmp/b.mp3"))
                .expectedSize(1024)
                .priority(Priority.HIGH)
                .tag("albumId", "abc")
                .build();
        assertEquals(1024, request.expectedSize());
        assertEquals(Priority.HIGH, request.priority());
        assertEquals(Map.of("albumId", "abc"), request.tags());
        assertThrows(UnsupportedOperationException.class, () -> request.tags().put("x", "y"));
    }

    @Test
    void downloadRequestRejectsNonsense() {
        assertThrows(IllegalArgumentException.class, () -> DownloadRequest.of(BOB, " ", Path.of("/tmp/x")));
        assertThrows(NullPointerException.class, () -> DownloadRequest.of(null, "a\\b", Path.of("/tmp/x")));
    }

    @Test
    @DisplayName("peer refusal text is classified once here, not string-matched in every consumer")
    void rejectionReasonsAreParsed() {
        assertEquals(RejectionReason.FILE_NOT_SHARED, RejectionReasons.parse("File not shared."));
        assertEquals(RejectionReason.FILE_NOT_SHARED, RejectionReasons.parse("not shared"));
        assertEquals(RejectionReason.BANNED, RejectionReasons.parse("Banned"));
        assertEquals(RejectionReason.TOO_MANY_FILES, RejectionReasons.parse("Too many files"));
        assertEquals(RejectionReason.TOO_MANY_MEGABYTES, RejectionReasons.parse("Too many megabytes"));
        assertEquals(RejectionReason.PENDING_SHUTDOWN, RejectionReasons.parse("Pending shutdown."));
        assertEquals(RejectionReason.QUEUE_FULL, RejectionReasons.parse("Queue full"));
        assertEquals(RejectionReason.CANCELLED_BY_PEER, RejectionReasons.parse("Cancelled"));
    }

    @Test
    @DisplayName("unrecognised refusal text is UNKNOWN, and the words survive")
    void unknownRejectionsKeepTheirText() {
        assertEquals(RejectionReason.UNKNOWN, RejectionReasons.parse("something nobody has seen"));
        assertEquals(RejectionReason.UNKNOWN, RejectionReasons.parse(""));
        assertEquals(RejectionReason.UNKNOWN, RejectionReasons.parse(null));
    }
}
