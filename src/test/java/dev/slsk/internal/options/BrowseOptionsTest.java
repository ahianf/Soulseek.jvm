// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BrowseOptionsTest {
    @Test
    @DisplayName("Instantiates properly")
    void instantiatesProperly() {
        Consumer<BrowseProgress> callback = progress -> {};

        BrowseOptions options = BrowseOptions.builder()
                .responseTimeout(Duration.ofMillis(-1))
                .progressUpdated(callback)
                .build();

        assertEquals(Duration.ofMillis(-1), options.responseTimeout());
        assertSame(callback, options.progressUpdated());
    }

    @Test
    @DisplayName("Uses source defaults")
    void usesSourceDefaults() {
        BrowseOptions options = new BrowseOptions();

        assertEquals(Duration.ofMinutes(1), options.responseTimeout());
        assertNull(options.progressUpdated());
    }

    @Test
    @DisplayName("Timeout-only overload leaves callback null")
    void timeoutOnlyOverloadLeavesCallbackNull() {
        BrowseOptions options =
                BrowseOptions.builder().responseTimeout(Duration.ofMillis(123)).build();

        assertEquals(Duration.ofMillis(123), options.responseTimeout());
        assertNull(options.progressUpdated());
    }

    @Test
    @DisplayName("Named callback and progress record preserve tuple behavior")
    void namedCallbackAndProgressRecordPreserveTupleBehavior() {
        AtomicReference<BrowseProgress> received = new AtomicReference<>();
        BrowseOptions options = BrowseOptions.builder()
                .responseTimeout(Duration.ofMillis(1))
                .progressUpdated(received::set)
                .build();
        BrowseProgress progress = new BrowseProgress("alice", 2, 3, 40, 5);

        options.progressUpdated().accept(progress);

        assertSame(progress, received.get());
        assertEquals(new BrowseProgress("alice", 2, 3, 40, 5), received.get());
    }
}
