// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.net.InetSocketAddress;

/**
 * The named Java representation of the C# distributed-peer tuple.
 *
 * @param username the peer username
 * @param ipEndPoint the peer endpoint
 */
public record DistributedPeer(String username, InetSocketAddress ipEndPoint) {}
