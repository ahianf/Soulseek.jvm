// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.MessageReceivedEvent;

/** A browse-response header and the connection carrying its body. */
public record BrowseResponseConnection(MessageReceivedEvent eventData, MessageConnection connection) {}
