// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BrowseOptionsTest {
    @Test
    @DisplayName("Instantiates properly")
    void instantiatesProperly() {
        BrowseProgressCallback callback = progress -> {};

        BrowseOptions options = new BrowseOptions(-1, callback);

        assertEquals(-1, options.getResponseTimeout());
        assertSame(callback, options.getProgressUpdated());
    }

    @Test
    @DisplayName("Uses source defaults")
    void usesSourceDefaults() {
        BrowseOptions options = new BrowseOptions();

        assertEquals(60_000, options.getResponseTimeout());
        assertNull(options.getProgressUpdated());
    }

    @Test
    @DisplayName("Timeout-only overload leaves callback null")
    void timeoutOnlyOverloadLeavesCallbackNull() {
        BrowseOptions options = new BrowseOptions(123);

        assertEquals(123, options.getResponseTimeout());
        assertNull(options.getProgressUpdated());
    }

    @Test
    @DisplayName("Named callback and progress record preserve tuple behavior")
    void namedCallbackAndProgressRecordPreserveTupleBehavior() {
        AtomicReference<BrowseProgress> received = new AtomicReference<>();
        BrowseOptions options = new BrowseOptions(1, received::set);
        BrowseProgress progress = new BrowseProgress("alice", 2, 3, 40, 5);

        options.getProgressUpdated().onProgressUpdated(progress);

        assertSame(progress, received.get());
        assertEquals(new BrowseProgress("alice", 2, 3, 40, 5), received.get());
    }
}
