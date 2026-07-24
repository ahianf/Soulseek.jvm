// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.handlers;

import dev.slsk.network.MessageConnection;
import dev.slsk.network.MessageReceivedEvent;

/** A browse-response header and the connection carrying its body. */
public record BrowseResponseConnection(MessageReceivedEvent eventArgs, MessageConnection connection) {}
