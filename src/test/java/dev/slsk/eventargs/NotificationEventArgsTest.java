// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class NotificationEventArgsTest {
    @Test
    @DisplayName("PrivateMessageReceivedEventArgs instantiates with expected values")
    void privateMessageInstantiatesWithExpectedValues() {
        Instant timestamp = Instant.parse("2026-07-23T12:34:56Z");
        PrivateMessageReceivedEventArgs args =
                new PrivateMessageReceivedEventArgs(42, timestamp, "alice", "hello", true);

        assertEquals(42, args.getId());
        assertSame(timestamp, args.getTimestamp());
        assertEquals("alice", args.getUsername());
        assertEquals("hello", args.getMessage());
        assertTrue(args.isReplayed());
    }

    @Test
    @DisplayName("PrivateMessageReceivedEventArgs preserves nullable references")
    void privateMessagePreservesNullableReferences() {
        PrivateMessageReceivedEventArgs args = new PrivateMessageReceivedEventArgs(0, null, null, null, false);

        assertNull(args.getTimestamp());
        assertNull(args.getUsername());
        assertNull(args.getMessage());
        assertFalse(args.isReplayed());
    }

    @Test
    @DisplayName("Privilege notification defaults to no acknowledgement")
    void privilegeNotificationDefaultsToNoAcknowledgement() {
        PrivilegeNotificationReceivedEventArgs args = new PrivilegeNotificationReceivedEventArgs("alice");

        assertEquals("alice", args.getUsername());
        assertNull(args.getId());
        assertFalse(args.isRequiresAcknowlegement());
    }

    @Test
    @DisplayName("Privilege notification requires acknowledgement when id is present")
    void privilegeNotificationRequiresAcknowledgementWhenIdPresent() {
        PrivilegeNotificationReceivedEventArgs args = new PrivilegeNotificationReceivedEventArgs("alice", 42);

        assertEquals(42, args.getId());
        assertTrue(args.isRequiresAcknowlegement());
    }

    @Test
    @DisplayName("Privilege notification preserves a nullable username")
    void privilegeNotificationPreservesNullableUsername() {
        PrivilegeNotificationReceivedEventArgs args = new PrivilegeNotificationReceivedEventArgs(null);

        assertNull(args.getUsername());
    }

    @Test
    @DisplayName("PublicChatMessageReceivedEventArgs instantiates with expected values")
    void publicChatInstantiatesWithExpectedValues() {
        PublicChatMessageReceivedEventArgs args = new PublicChatMessageReceivedEventArgs("lobby", "alice", "hello");

        assertEquals("lobby", args.getRoomName());
        assertEquals("alice", args.getUsername());
        assertEquals("hello", args.getMessage());
    }

    @Test
    @DisplayName("PublicChatMessageReceivedEventArgs preserves nullable references")
    void publicChatPreservesNullableReferences() {
        PublicChatMessageReceivedEventArgs args = new PublicChatMessageReceivedEventArgs(null, null, null);

        assertNull(args.getRoomName());
        assertNull(args.getUsername());
        assertNull(args.getMessage());
    }
}
