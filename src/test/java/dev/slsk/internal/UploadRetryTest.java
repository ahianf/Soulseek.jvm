// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.Username;
import dev.slsk.internal.common.Eventually;
import dev.slsk.internal.common.Scheduler;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class UploadRetryTest {

    private static final Username ALICE = Username.of("alice");
    private static final String PATH = "music\\song.mp3";

    /** Short enough that a booked re-offer fires within the test's patience. */
    private static final Duration DELAY = Duration.ofMillis(10);

    private final Scheduler scheduler = new Scheduler("upload-retry-test");
    private final List<String> reoffered = new CopyOnWriteArrayList<>();
    private final UploadRetry retry = new UploadRetry(
            scheduler, DELAY, 2, (user, path) -> reoffered.add(user.value() + ":" + path), new DiagnosticSink() {
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
                public void warning(String message) {}

                @Override
                public void warning(String message, Throwable exception) {}
            });

    @AfterEach
    void closeScheduler() {
        scheduler.close();
    }

    /** A retryable failure books exactly one re-offer, delivered after the delay. */
    @Test
    void failureBooksOneReoffer() {
        retry.failed(ALICE, PATH);
        assertTrue(Eventually.holds(() -> reoffered.size() == 1));
        assertEquals("alice:" + PATH, reoffered.get(0));
    }

    /**
     * The budget is per file: attempts at one file spend nothing from another,
     * so a struggling peer cannot starve retries for the rest of the queue.
     */
    @Test
    void budgetIsPerFile() {
        retry.failed(ALICE, PATH);
        retry.failed(ALICE, "music\\other.mp3");
        assertTrue(Eventually.holds(() -> reoffered.size() == 2));
    }

    /**
     * After the attempt budget is spent, a further failure books nothing: the
     * record is dropped and recovery is the peer's own next request.
     */
    @Test
    void givesUpAfterMaxAttempts() {
        retry.failed(ALICE, PATH);
        retry.failed(ALICE, PATH);
        assertTrue(Eventually.holds(() -> reoffered.size() == 2));
        retry.failed(ALICE, PATH);
        sleepPastDelay();
        assertEquals(2, reoffered.size());
    }

    /**
     * Success clears the record, so the next failure starts a fresh budget.
     * Without the success the third failure would overrun the budget of two
     * and book nothing, so the third re-offer is what proves the reset.
     */
    @Test
    void successResetsTheBudget() {
        retry.failed(ALICE, PATH);
        retry.failed(ALICE, PATH);
        assertTrue(Eventually.holds(() -> reoffered.size() == 2));

        retry.succeeded(ALICE, PATH);
        retry.failed(ALICE, PATH);
        assertTrue(Eventually.holds(() -> reoffered.size() == 3));
    }

    /** An abandoned file's next failure also starts fresh, like success. */
    @Test
    void abandonmentResetsTheBudget() {
        retry.failed(ALICE, PATH);
        retry.failed(ALICE, PATH);
        assertTrue(Eventually.holds(() -> reoffered.size() == 2));

        retry.abandoned(ALICE, PATH);
        retry.failed(ALICE, PATH);
        assertTrue(Eventually.holds(() -> reoffered.size() == 3));
    }

    /**
     * Waits out several delay lengths, so "nothing fired" is a statement about
     * a booking that never existed rather than one still pending.
     */
    private void sleepPastDelay() {
        try {
            Thread.sleep(DELAY.toMillis() * 10);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
