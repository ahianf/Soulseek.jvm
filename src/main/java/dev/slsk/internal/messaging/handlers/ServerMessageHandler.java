// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

import dev.slsk.Subscription;
import java.util.function.Consumer;

/** Handles messages received from the server connection. */
public interface ServerMessageHandler extends MessageHandler {
    <T> Subscription subscribe(ServerMessageEvent event, Consumer<? super T> listener);
}
