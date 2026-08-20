// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

/** Identifies an event payload emitted by the internal client machinery. */
public sealed interface SoulseekClientEvent
        permits BrowseEvent,
                BrowseProgressUpdatedEvent,
                DistributedChildEvent,
                DistributedParentEvent,
                DownloadDeniedEvent,
                DownloadFailedEvent,
                PrivateMessageReceivedEvent,
                PrivilegeNotificationReceivedEvent,
                PublicChatMessageReceivedEvent,
                RoomJoinedEvent,
                RoomLeftEvent,
                RoomMessageReceivedEvent,
                RoomTickerAddedEvent,
                RoomTickerListReceivedEvent,
                RoomTickerRemovedEvent,
                SearchRequestEvent,
                SearchRequestResponseEvent,
                SearchResponseReceivedEvent,
                SearchStateChangedEvent,
                SoulseekClientDisconnectedEvent,
                SoulseekClientStateChangedEvent,
                UserCannotConnectEvent {}
