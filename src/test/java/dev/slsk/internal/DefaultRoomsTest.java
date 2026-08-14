// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.Attachment;
import dev.slsk.CancellationSignal;
import dev.slsk.Soulseek;
import dev.slsk.events.RoomEvent;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.room.Room;
import dev.slsk.room.RoomTicker;
import dev.slsk.user.Username;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DefaultRoomsTest {

    private static Soulseek client() {
        return DefaultSoulseek.create("alice", "password", 157, new SoulseekClientOptions());
    }

    @Test
    void joinedStartsEmpty() {
        try (Soulseek slsk = client()) {
            assertEquals(List.of(), slsk.rooms().joined());
        }
    }

    @Test
    @DisplayName("asking for a room we are not in is a programming error, not an empty room")
    void getOnAnUnjoinedRoomThrows() {
        try (Soulseek slsk = client()) {
            assertThrows(IllegalArgumentException.class, () -> slsk.rooms().get("nowhere"));
        }
    }

    @Test
    @DisplayName("leaving a room we are not in does nothing rather than failing")
    void leaveIsAnIdempotentIntent() {
        try (Soulseek slsk = client()) {
            slsk.rooms().leave("nowhere");
            slsk.rooms().leave("nowhere");
            assertEquals(List.of(), slsk.rooms().joined());
        }
    }

    @Test
    void attachReturnsTheJoinedRoomsAndSubscribes() {
        try (Soulseek slsk = client()) {
            try (Attachment<List<Room>> attached = slsk.rooms().attach(event -> {})) {
                assertEquals(List.of(), attached.state());
            }
        }
    }

    @Test
    void rejectsNullArguments() {
        try (Soulseek slsk = client()) {
            assertThrows(NullPointerException.class, () -> slsk.rooms().list(null));
            assertThrows(NullPointerException.class, () -> slsk.rooms().join(null, CancellationSignal.none()));
            assertThrows(NullPointerException.class, () -> slsk.rooms().join("r", null));
            assertThrows(NullPointerException.class, () -> slsk.rooms().leave(null));
            assertThrows(NullPointerException.class, () -> slsk.rooms().say(null, "m"));
            assertThrows(NullPointerException.class, () -> slsk.rooms().say("r", null));
            assertThrows(NullPointerException.class, () -> slsk.rooms().setTicker("r", null));
            assertThrows(NullPointerException.class, () -> slsk.rooms().get(null));
        }
    }

    @Test
    void exposesPrivateRoomAdministration() {
        try (Soulseek slsk = client()) {
            assertTrue(slsk.rooms().privateRooms() != null);
        }
    }

    @Test
    @DisplayName("a Room carries membership and tickers, and deliberately carries no messages")
    void roomHoldsStateNotHistory() {
        Room room = new Room(
                "lobby",
                List.of(),
                List.of(new RoomTicker(Username.of("bob"), "hello")),
                false,
                Optional.empty(),
                Set.of());
        assertEquals(0, room.userCount());
        assertEquals(1, room.tickers().size());
        // There is no accessor for messages, by design: history is the
        // application's. This asserts the shape rather than a behaviour.
        assertEquals(6, Room.class.getRecordComponents().length);
    }

    @Test
    void roomCollectionsAreImmutable() {
        Room room = new Room("lobby", List.of(), List.of(), false, Optional.empty(), Set.of());
        assertThrows(UnsupportedOperationException.class, () -> room.tickers().add(null));
        assertThrows(UnsupportedOperationException.class, () -> room.users().add(null));
        assertThrows(UnsupportedOperationException.class, () -> room.operators().add(null));
    }

    @Test
    @DisplayName("a switch over RoomEvent needs no default")
    void roomEventIsExhaustivelySwitchable() {
        RoomEvent event = new RoomEvent.MessageReceived("lobby", Username.of("bob"), "hi", Instant.EPOCH);
        String rendered =
                switch (event) {
                    case RoomEvent.Joined joined -> "joined";
                    case RoomEvent.Left left -> "left";
                    case RoomEvent.MessageReceived message -> message.from() + ": " + message.message();
                    case RoomEvent.UserJoined userJoined -> "user joined";
                    case RoomEvent.UserLeft userLeft -> "user left";
                    case RoomEvent.TickerAdded tickerAdded -> "ticker added";
                    case RoomEvent.TickerRemoved tickerRemoved -> "ticker removed";
                    case RoomEvent.TickerListReceived tickerList -> "tickers";
                    case RoomEvent.ListReceived listReceived -> "room list";
                    case RoomEvent.PublicChatMessageReceived publicChat -> "public";
                    case RoomEvent.MembershipAdded membershipAdded -> "membership added";
                    case RoomEvent.MembershipRemoved membershipRemoved -> "membership removed";
                    case RoomEvent.ModerationAdded moderationAdded -> "moderation added";
                    case RoomEvent.ModerationRemoved moderationRemoved -> "moderation removed";
                };
        assertEquals("bob: hi", rendered);
    }
}
