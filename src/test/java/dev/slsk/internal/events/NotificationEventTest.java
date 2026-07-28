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

        assertEquals(42, args.getId());
        assertSame(timestamp, args.getTimestamp());
        assertEquals("alice", args.getUsername());
        assertEquals("hello", args.getMessage());
        assertTrue(args.isReplayed());
    }

    @Test
    @DisplayName("PrivateMessageReceivedEvent preserves nullable references")
    void privateMessagePreservesNullableReferences() {
        PrivateMessageReceivedEvent args = new PrivateMessageReceivedEvent(0, null, null, null, false);

        assertNull(args.getTimestamp());
        assertNull(args.getUsername());
        assertNull(args.getMessage());
        assertFalse(args.isReplayed());
    }

    @Test
    @DisplayName("Privilege notification defaults to no acknowledgement")
    void privilegeNotificationDefaultsToNoAcknowledgement() {
        PrivilegeNotificationReceivedEvent args = new PrivilegeNotificationReceivedEvent("alice");

        assertEquals("alice", args.getUsername());
        assertNull(args.getId());
        assertFalse(args.isRequiresAcknowlegement());
    }

    @Test
    @DisplayName("Privilege notification requires acknowledgement when id is present")
    void privilegeNotificationRequiresAcknowledgementWhenIdPresent() {
        PrivilegeNotificationReceivedEvent args = new PrivilegeNotificationReceivedEvent("alice", 42);

        assertEquals(42, args.getId());
        assertTrue(args.isRequiresAcknowlegement());
    }

    @Test
    @DisplayName("Privilege notification preserves a nullable username")
    void privilegeNotificationPreservesNullableUsername() {
        PrivilegeNotificationReceivedEvent args = new PrivilegeNotificationReceivedEvent(null);

        assertNull(args.getUsername());
    }

    @Test
    @DisplayName("PublicChatMessageReceivedEvent instantiates with expected values")
    void publicChatInstantiatesWithExpectedValues() {
        PublicChatMessageReceivedEvent args = new PublicChatMessageReceivedEvent("lobby", "alice", "hello");

        assertEquals("lobby", args.getRoomName());
        assertEquals("alice", args.getUsername());
        assertEquals("hello", args.getMessage());
    }

    @Test
    @DisplayName("PublicChatMessageReceivedEvent preserves nullable references")
    void publicChatPreservesNullableReferences() {
        PublicChatMessageReceivedEvent args = new PublicChatMessageReceivedEvent(null, null, null);

        assertNull(args.getRoomName());
        assertNull(args.getUsername());
        assertNull(args.getMessage());
    }
}
