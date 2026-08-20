// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.internal.network.tcp.TransportConnection;

/**
 * A transfer connection and the peer's transfer token.
 *
 * @param connection the transfer connection
 * @param remoteToken the peer token
 */
public record TransferConnectionResult(TransportConnection connection, int remoteToken) {}
