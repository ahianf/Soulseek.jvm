// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.network.tcp.IConnection;

/**
 * A transfer connection and the peer's transfer token.
 *
 * @param connection the transfer connection
 * @param remoteToken the peer token
 */
public record TransferConnectionResult(IConnection connection, int remoteToken) {}
