// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.room.RoomTicker;

/** Event payload emitted when a ticker is added to a chat room. */
public record RoomTickerAddedEvent(String roomName, RoomTicker ticker) implements SoulseekClientEvent {}
