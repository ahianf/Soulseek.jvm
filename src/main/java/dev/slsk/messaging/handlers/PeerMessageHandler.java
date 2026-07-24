// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.handlers;

import dev.slsk.eventargs.DownloadDeniedEventArgs;
import dev.slsk.eventargs.DownloadFailedEventArgs;
import dev.slsk.network.IMessageConnection;
import dev.slsk.network.MessageReceivedEventArgs;

/** Handles messages received from peer connections. */
public interface PeerMessageHandler extends MessageHandler {
    void addDownloadDeniedListener(PeerMessageHandlerEventListener<DownloadDeniedEventArgs> listener);

    void removeDownloadDeniedListener(PeerMessageHandlerEventListener<DownloadDeniedEventArgs> listener);

    void addDownloadFailedListener(PeerMessageHandlerEventListener<DownloadFailedEventArgs> listener);

    void removeDownloadFailedListener(PeerMessageHandlerEventListener<DownloadFailedEventArgs> listener);

    void handleMessageReceived(IMessageConnection sender, MessageReceivedEventArgs eventArgs);
}
