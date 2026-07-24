// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BrowseEventArgsTest {
    @Test
    @DisplayName("BrowseEventArgs instantiates with the given data")
    void browseInstantiatesWithGivenData() {
        BrowseEventArgs args = new BrowseEventArgs("alice");

        assertEquals("alice", args.getUsername());
    }

    @Test
    @DisplayName("BrowseProgressUpdatedEventArgs instantiates with the given data")
    void progressInstantiatesWithGivenData() {
        BrowseProgressUpdatedEventArgs args = new BrowseProgressUpdatedEventArgs("alice", 25, 100);

        assertEquals("alice", args.getUsername());
        assertEquals(25, args.getBytesTransferred());
        assertEquals(100, args.getSize());
        assertEquals(75, args.getBytesRemaining());
        assertEquals(25.0d, args.getPercentComplete());
    }

    @Test
    @DisplayName("Browse event arguments preserve a nullable username")
    void preservesNullableUsername() {
        assertNull(new BrowseEventArgs(null).getUsername());
        assertNull(new BrowseProgressUpdatedEventArgs(null, 1, 2).getUsername());
    }

    @Test
    @DisplayName("Browse progress preserves source zero-size arithmetic")
    void progressPreservesZeroSizeArithmetic() {
        BrowseProgressUpdatedEventArgs zero = new BrowseProgressUpdatedEventArgs("alice", 0, 0);
        BrowseProgressUpdatedEventArgs positive = new BrowseProgressUpdatedEventArgs("alice", 1, 0);

        assertTrue(Double.isNaN(zero.getPercentComplete()));
        assertEquals(Double.POSITIVE_INFINITY, positive.getPercentComplete());
        assertEquals(-1, positive.getBytesRemaining());
    }
}
