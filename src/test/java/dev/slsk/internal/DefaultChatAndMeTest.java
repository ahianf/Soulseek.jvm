// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationSignal;
import dev.slsk.Soulseek;
import dev.slsk.events.ChatEvent;
import dev.slsk.events.MeEvent;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.user.UserPresence;
import dev.slsk.user.UserProfile;
import dev.slsk.user.Username;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DefaultChatAndMeTest {

    private static Soulseek client() {
        return DefaultSoulseek.create("alice", "password", 157, new SoulseekClientOptions());
    }

    @Test
    void reportsTheAccountItWasBuiltWith() {
        try (Soulseek slsk = client()) {
            assertEquals(Username.of("alice"), slsk.me().username());
        }
    }

    @Test
    @DisplayName("presence starts online and is readable, which the protocol cannot answer")
    void tracksPresenceLocally() {
        try (Soulseek slsk = client()) {
            assertEquals(UserPresence.ONLINE, slsk.me().presence());
        }
    }

    @Test
    @DisplayName("setting the presence we already have is a no-op, not a wire message")
    void presenceIsAnIdempotentIntent() {
        try (Soulseek slsk = client()) {
            List<MeEvent> events = new ArrayList<>();
            slsk.me().events().subscribe(events::add);

            // Already ONLINE. Setting it again must not reach the client, which
            // would throw because we are not connected.
            slsk.me().presence(UserPresence.ONLINE);

            assertEquals(UserPresence.ONLINE, slsk.me().presence());
            assertTrue(events.isEmpty(), "an unchanged presence raises nothing");
        }
    }

    @Test
    void rejectsNullAndNonsenseArguments() {
        try (Soulseek slsk = client()) {
            assertThrows(NullPointerException.class, () -> slsk.me().presence(null));
            assertThrows(NullPointerException.class, () -> slsk.chat().send(null, "m", CancellationSignal.none()));
            assertThrows(
                    NullPointerException.class,
                    () -> slsk.chat().send(Username.of("bob"), null, CancellationSignal.none()));
            assertThrows(NullPointerException.class, () -> slsk.chat().send(Username.of("bob"), "m", null));
            assertThrows(NullPointerException.class, () -> slsk.me().privileges(null));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> slsk.me().giftPrivileges(Username.of("bob"), 0, CancellationSignal.none()));
            assertThrows(IllegalArgumentException.class, () -> slsk.me().reportUploadSpeed(-1));
        }
    }

    @Test
    void exposesChatAndMeEventStreams() {
        try (Soulseek slsk = client()) {
            List<ChatEvent> chat = new ArrayList<>();
            List<MeEvent> me = new ArrayList<>();
            try (var first = slsk.chat().events().subscribe(chat::add);
                    var second = slsk.me().events().subscribe(me::add)) {
                assertTrue(chat.isEmpty());
                assertTrue(me.isEmpty());
            }
        }
    }

    @Test
    @DisplayName("a switch over ChatEvent and MeEvent needs no default")
    void eventHierarchiesAreExhaustivelySwitchable() {
        ChatEvent chat = new ChatEvent.MessageReceived(
                Username.of("bob"), "hello", false, java.time.Instant.EPOCH, java.time.Instant.EPOCH);
        String renderedChat =
                switch (chat) {
                    case ChatEvent.MessageReceived received -> received.from() + ": " + received.message();
                };
        assertEquals("bob: hello", renderedChat);

        MeEvent me = new MeEvent.PresenceChanged(UserPresence.ONLINE, UserPresence.AWAY, java.time.Instant.EPOCH);
        String renderedMe =
                switch (me) {
                    case MeEvent.LoggedIn loggedIn -> "logged in";
                    case MeEvent.PrivilegeNotificationReceived received -> "privileges";
                    case MeEvent.PrivilegedUserListReceived received -> "list";
                    case MeEvent.PresenceChanged changed -> changed.from() + " -> " + changed.to();
                };
        assertEquals("ONLINE -> AWAY", renderedMe);
    }

    /**
     * The profile is a value, not a callback. What a peer sees is what was set,
     * and what was set survives being read back — the old resolver could not be
     * read back at all, so an application that wanted to render its own profile
     * had to remember what it had configured.
     */
    @Test
    @DisplayName("the profile is set once and served to every peer who asks")
    void profileIsAValueRatherThanACallback() {
        try (Soulseek slsk = client()) {
            assertEquals(UserProfile.empty(), slsk.me().profile());

            UserProfile mine = new UserProfile("hello", Optional.of(new byte[] {1, 2}), 4, 9, true);
            slsk.me().profile(mine);

            assertEquals(mine, slsk.me().profile());
            assertEquals(mine, ((DefaultSoulseek) slsk).client().profile());

            assertThrows(NullPointerException.class, () -> slsk.me().profile(null));
            assertEquals(mine, slsk.me().profile(), "a rejected set leaves the old profile in place");
        }
    }

    @Test
    void aProfileRejectsCountsThatCannotBeTrue() {
        assertThrows(IllegalArgumentException.class, () -> new UserProfile("", Optional.empty(), -1, 0, false));
        assertThrows(IllegalArgumentException.class, () -> new UserProfile("", Optional.empty(), 0, -1, false));
        assertThrows(NullPointerException.class, () -> new UserProfile(null, Optional.empty(), 0, 0, false));
        assertEquals("only a description", UserProfile.of("only a description").description());
    }
}
