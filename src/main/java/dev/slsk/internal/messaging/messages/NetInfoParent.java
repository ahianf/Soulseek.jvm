// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import java.net.InetAddress;

/**
 * Named Java representation of the net-info parent tuple.
 *
 * @param username the candidate username
 * @param ipAddress the candidate address
 * @param port the candidate port
 */
public record NetInfoParent(String username, InetAddress ipAddress, int port) {}
