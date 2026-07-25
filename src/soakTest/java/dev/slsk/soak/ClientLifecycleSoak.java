// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.soak;

import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.SoulseekClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Scenario: repeated client open and close.
 *
 * <p>Measures the platform-thread cost of one client and asserts that closing
 * gives all of it back. A client currently owns several independent
 * single-thread schedulers — the waiter timeout thread, the cleanup scheduler,
 * two token buckets — which is defect 1.6. Consolidating them should reduce the
 * per-client cost measured here; releasing them must keep working either way.
 *
 * <p>No network: constructing a client starts its schedulers without
 * connecting, which is exactly the lifecycle under test.
 */
class ClientLifecycleSoak {

    private static final int ITERATIONS = 25;
    private static final int CONCURRENT_CLIENTS = 8;

    @Test
    @DisplayName("Per-client platform thread cost")
    void perClientThreadCost() throws Exception {
        int atRest = ThreadCensus.libraryThreadCount();
        SoakReport.record("client-lifecycle", "library platform threads at rest", atRest);

        List<SoulseekClient> clients = new ArrayList<>(CONCURRENT_CLIENTS);
        try {
            for (int index = 0; index < CONCURRENT_CLIENTS; index++) {
                clients.add(SoulseekClient.create(157));
            }
            int withClients = ThreadCensus.libraryThreadCount();
            SoakReport.record(
                    "client-lifecycle", "library platform threads @ " + CONCURRENT_CLIENTS + " clients", withClients);
            SoakReport.record(
                    "client-lifecycle",
                    "platform threads per client",
                    String.format(Locale.ROOT, "%.1f", (withClients - atRest) / (double) CONCURRENT_CLIENTS));
            SoakReport.note("client-lifecycle", "census with clients: " + ThreadCensus.describe());
        } finally {
            for (SoulseekClient client : clients) {
                client.close();
            }
        }

        int settled = ThreadCensus.awaitLibraryThreadsAtMost(atRest + 2, 30, TimeUnit.SECONDS);
        SoakReport.record("client-lifecycle", "library platform threads after close", settled);

        assertTrue(
                settled <= atRest + 2,
                "Closing every client did not release its threads: at rest=" + atRest + " after=" + settled + " census="
                        + ThreadCensus.describe());
    }

    @Test
    @DisplayName("Repeated open and close does not leak threads")
    void repeatedLifecycleDoesNotLeak() throws Exception {
        int atRest = ThreadCensus.libraryThreadCount();

        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            try (SoulseekClient client = SoulseekClient.create(157)) {
                // Constructing and closing is the whole lifecycle under test.
                assertTrue(client.getState() != null, "A newly created client must report a state.");
            }
        }

        int settled = ThreadCensus.awaitLibraryThreadsAtMost(atRest + 2, 30, TimeUnit.SECONDS);
        SoakReport.record("client-lifecycle", "iterations", ITERATIONS);
        SoakReport.record("client-lifecycle", "library platform threads after " + ITERATIONS + " cycles", settled);

        assertTrue(
                settled <= atRest + 2,
                "Threads leaked across " + ITERATIONS + " open/close cycles: at rest=" + atRest + " after=" + settled
                        + " census=" + ThreadCensus.describe());
    }
}
