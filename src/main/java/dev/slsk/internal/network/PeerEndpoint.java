// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import java.net.InetSocketAddress;

/**
 * A peer username and endpoint.
 *
 * @param username the peer username
 * @param ipEndpoint the peer endpoint
 */
public record PeerEndpoint(String username, InetSocketAddress ipEndpoint) {}
