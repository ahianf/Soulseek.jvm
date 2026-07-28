// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.Progress;
import dev.slsk.TransferId;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The cadence and the smoothing, asserted on a clock that can be moved rather
 * than waited out.
 */
class ProgressCoalescerTest {

    private static final TransferId ID = TransferId.of("DOWNLOAD:1");
    private static final long MILLIS = Duration.ofMillis(1).toNanos();

    private final AtomicLong clock = new AtomicLong();
    private final ProgressCoalescer coalescer = new ProgressCoalescer(clock::get);

    private void advance(long millis) {
        clock.addAndGet(millis * MILLIS);
    }

    @Test
    @DisplayName("the first reading publishes, so a transfer does not start invisible")
    void theFirstReadingAlwaysPublishes() {
        assertTrue(coalescer.offer(ID, 0, 1000).isPresent());
    }

    @Test
    @DisplayName("readings inside the cadence are dropped")
    void publishesAtMostOncePerCadence() {
        coalescer.offer(ID, 0, 10_000);

        int published = 0;
        for (int read = 1; read <= 1000; read++) {
            advance(1);
            if (coalescer.offer(ID, read, 10_000).isPresent()) {
                published++;
            }
        }

        // One second of reads at a thousand a second: four publishes, not a
        // thousand. A UI must never be the thing that throttles the network.
        assertEquals(4, published);
    }

    @Test
    @DisplayName("the last byte publishes whatever the cadence says")
    void completionAlwaysPublishes() {
        coalescer.offer(ID, 0, 100);
        advance(1);
        assertFalse(coalescer.offer(ID, 50, 100).isPresent(), "halfway, and too soon");
        advance(1);
        // A consumer that never sees 100% renders a bar that stops just short.
        assertTrue(coalescer.offer(ID, 100, 100).isPresent());
    }

    @Test
    @DisplayName("a steady transfer reports the rate it is actually going at")
    void theRateConvergesOnASteadyStream() {
        coalescer.offer(ID, 0, 10_000_000);
        long transferred = 0;
        Progress last = null;
        // 1 MiB/s, offered as a reading every 10 ms.
        for (int tick = 0; tick < 300; tick++) {
            advance(10);
            transferred += 10_486;
            Optional<Progress> published = coalescer.offer(ID, transferred, 10_000_000);
            if (published.isPresent()) {
                last = published.get();
            }
        }

        assertTrue(last != null);
        assertEquals(1_048_600, last.bytesPerSecond(), 20_000, "about a mebibyte a second");
    }

    /**
     * The reason smoothing is not a convenience. Bytes arrive in whatever sizes
     * the network hands over, so a rate taken from one read swings wildly; a
     * consumer showing it would show a number that is simply wrong.
     */
    @Test
    @DisplayName("one enormous read does not make the reported rate jump to it")
    void aSingleBurstDoesNotDominateTheRate() {
        coalescer.offer(ID, 0, 10_000_000);
        long transferred = 0;
        for (int tick = 0; tick < 99; tick++) {
            advance(10);
            transferred += 10_000;
            coalescer.offer(ID, transferred, 10_000_000);
        }

        // Lands on a publish boundary, so the burst is what gets reported.
        advance(10);
        transferred += 1_000_000;
        Progress spiked = coalescer.offer(ID, transferred, 10_000_000).orElseThrow();

        // The raw sample is a hundred megabytes a second. What is reported is
        // nearer the truth of the last second than the truth of the last read.
        assertTrue(
                spiked.bytesPerSecond() < 30_000_000,
                "a single burst reported as " + spiked.bytesPerSecond() + " bytes per second");
        assertTrue(spiked.bytesPerSecond() > 1_000_000, "and it is not ignored either");
    }

    @Test
    void transfersDoNotShareARate() {
        TransferId other = TransferId.of("DOWNLOAD:2");
        coalescer.offer(ID, 0, 1000);
        coalescer.offer(other, 0, 1000);

        advance(250);
        Progress first = coalescer.offer(ID, 1000, 1000).orElseThrow();
        Progress second = coalescer.offer(other, 10, 1000).orElseThrow();

        assertTrue(first.bytesPerSecond() > second.bytesPerSecond());
    }

    @Test
    @DisplayName("a forgotten transfer starts over rather than resuming someone else's rate")
    void forgettingClearsTheTrack() {
        coalescer.offer(ID, 0, 1000);
        advance(250);
        coalescer.offer(ID, 1000, 1000);

        coalescer.forget(ID);
        assertTrue(coalescer.offer(ID, 0, 1000).isPresent(), "the first reading of a new attempt publishes");
    }
}
