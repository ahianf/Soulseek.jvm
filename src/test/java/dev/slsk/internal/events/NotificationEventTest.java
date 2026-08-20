// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class NotificationEventTest {
    @Test
    @DisplayName("PrivateMessageReceivedEvent instantiates with expected values")
    void privateMessageInstantiatesWithExpectedValues() {
        Instant timestamp = Instant.parse("2026-07-23T12:34:56Z");
        PrivateMessageReceivedEvent args = new PrivateMessageReceivedEvent(42, timestamp, "alice", "hello", true);

        assertEquals(42, args.id());
        assertSame(timestamp, args.timestamp());
        assertEquals("alice", args.username());
        assertEquals("hello", args.message());
        assertTrue(args.replayed());
    }

    @Test
    @DisplayName("PrivateMessageReceivedEvent preserves nullable references")
    void privateMessagePreservesNullableReferences() {
        PrivateMessageReceivedEvent args = new PrivateMessageReceivedEvent(0, null, null, null, false);

        assertNull(args.timestamp());
        assertNull(args.username());
        assertNull(args.message());
        assertFalse(args.replayed());
    }

    @Test
    @DisplayName("Privilege notification defaults to no acknowledgement")
    void privilegeNotificationDefaultsToNoAcknowledgement() {
        PrivilegeNotificationReceivedEvent args = new PrivilegeNotificationReceivedEvent("alice");

        assertEquals("alice", args.username());
        assertNull(args.id());
        assertFalse(args.requiresAcknowledgement());
    }

    @Test
    @DisplayName("Privilege notification requires acknowledgement when id is present")
    void privilegeNotificationRequiresAcknowledgementWhenIdPresent() {
        PrivilegeNotificationReceivedEvent args = new PrivilegeNotificationReceivedEvent("alice", 42);

        assertEquals(42, args.id());
        assertTrue(args.requiresAcknowledgement());
    }

    @Test
    @DisplayName("Privilege notification preserves a nullable username")
    void privilegeNotificationPreservesNullableUsername() {
        PrivilegeNotificationReceivedEvent args = new PrivilegeNotificationReceivedEvent(null);

        assertNull(args.username());
    }

    @Test
    @DisplayName("PublicChatMessageReceivedEvent instantiates with expected values")
    void publicChatInstantiatesWithExpectedValues() {
        PublicChatMessageReceivedEvent args = new PublicChatMessageReceivedEvent("lobby", "alice", "hello");

        assertEquals("lobby", args.roomName());
        assertEquals("alice", args.username());
        assertEquals("hello", args.message());
    }

    @Test
    @DisplayName("PublicChatMessageReceivedEvent preserves nullable references")
    void publicChatPreservesNullableReferences() {
        PublicChatMessageReceivedEvent args = new PublicChatMessageReceivedEvent(null, null, null);

        assertNull(args.roomName());
        assertNull(args.username());
        assertNull(args.message());
    }
}
