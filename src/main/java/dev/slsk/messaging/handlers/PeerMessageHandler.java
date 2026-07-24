// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.handlers;

import dev.slsk.events.DownloadDeniedEvent;
import dev.slsk.events.DownloadFailedEvent;
import dev.slsk.network.MessageConnection;
import dev.slsk.network.MessageReceivedEvent;

/** Handles messages received from peer connections. */
public interface PeerMessageHandler extends MessageHandler {
    void addDownloadDeniedListener(PeerMessageHandlerEventListener<DownloadDeniedEvent> listener);

    void removeDownloadDeniedListener(PeerMessageHandlerEventListener<DownloadDeniedEvent> listener);

    void addDownloadFailedListener(PeerMessageHandlerEventListener<DownloadFailedEvent> listener);

    void removeDownloadFailedListener(PeerMessageHandlerEventListener<DownloadFailedEvent> listener);

    void handleMessageReceived(MessageConnection sender, MessageReceivedEvent eventData);
}
