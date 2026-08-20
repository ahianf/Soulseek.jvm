// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

/** Event payload emitted when a peer reports that a download failed. */
public record DownloadFailedEvent(String username, String filename) implements SoulseekClientEvent {}
