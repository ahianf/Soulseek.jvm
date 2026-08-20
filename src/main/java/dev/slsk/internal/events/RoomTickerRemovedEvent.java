// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

/** Event payload emitted when a ticker is removed from a chat room. */
public record RoomTickerRemovedEvent(String roomName, String username) implements SoulseekClientEvent {}
