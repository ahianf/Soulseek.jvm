// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

import dev.slsk.Subscription;
import dev.slsk.internal.network.MessageReceivedEvent;
import java.util.function.Consumer;

/** Handles messages received from peer connections. */
public interface PeerMessageHandler extends MessageHandler {
    enum Kind {
        DOWNLOAD_DENIED,
        DOWNLOAD_FAILED
    }

    <T> Subscription subscribe(Kind kind, Consumer<? super T> listener);

    void handleMessageReceived(MessageReceivedEvent eventData);
}
