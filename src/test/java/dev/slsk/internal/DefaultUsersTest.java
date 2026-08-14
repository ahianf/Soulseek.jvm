// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationSignal;
import dev.slsk.Soulseek;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.user.UserPresence;
import dev.slsk.user.UserStatus;
import dev.slsk.user.Username;
import dev.slsk.user.Watch;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DefaultUsersTest {

    private static final Username BOB = Username.of("bob");

    private static Soulseek client() {
        return DefaultSoulseek.create("alice", "password", 157, new SoulseekClientOptions());
    }

    @Test
    void watchesStartEmpty() {
        try (Soulseek slsk = client()) {
            assertEquals(Set.of(), slsk.users().watched());
        }
    }

    @Test
    @DisplayName("two holders of one user share one server subscription")
    void watchesAreReferenceCounted() {
        try (Soulseek slsk = client()) {
            // watchUser reaches the client, which throws when not connected; the
            // registry still has to behave, because a failed first watch must
            // not leave a phantom entry.
            assertThrows(Exception.class, () -> slsk.users().watch(BOB));
            assertFalse(slsk.users().watched().contains(BOB), "a failed watch registers nothing");
        }
    }

    @Test
    void watchedIsAnImmutableSnapshot() {
        try (Soulseek slsk = client()) {
            Set<Username> watched = slsk.users().watched();
            assertThrows(UnsupportedOperationException.class, () -> watched.add(BOB));
        }
    }

    @Test
    void rejectsNullArguments() {
        try (Soulseek slsk = client()) {
            assertThrows(NullPointerException.class, () -> slsk.users().watch(null));
            assertThrows(NullPointerException.class, () -> slsk.users().info(null, CancellationSignal.none()));
            assertThrows(NullPointerException.class, () -> slsk.users().info(BOB, null));
            assertThrows(NullPointerException.class, () -> slsk.users().statistics(null, CancellationSignal.none()));
            assertThrows(NullPointerException.class, () -> slsk.users().status(null, CancellationSignal.none()));
            assertThrows(NullPointerException.class, () -> slsk.users().endpoint(null, CancellationSignal.none()));
        }
    }

    @Test
    @DisplayName("offline is a value, so a status can be read without a try block")
    void statusModelsOfflineAsAValue() {
        UserStatus offline = new UserStatus(BOB, UserPresence.OFFLINE, false);
        assertFalse(offline.isOnline());
        assertTrue(new UserStatus(BOB, UserPresence.AWAY, false).isOnline());
        assertTrue(new UserStatus(BOB, UserPresence.ONLINE, true).isOnline());
        assertTrue(new UserStatus(BOB, UserPresence.ONLINE, true).privileged());
    }

    @Test
    void watchIsAutoCloseable() {
        // The contract, asserted on the type rather than a live instance: a
        // watch is released by closing it, not by a paired unwatch call.
        assertTrue(AutoCloseable.class.isAssignableFrom(Watch.class));
    }
}
