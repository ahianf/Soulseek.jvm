// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import java.net.InetSocketAddress;

/** Event payload emitted when a distributed child connection changes. */
public record DistributedChildEvent(String username, InetSocketAddress ipEndpoint) implements SoulseekClientEvent {}
