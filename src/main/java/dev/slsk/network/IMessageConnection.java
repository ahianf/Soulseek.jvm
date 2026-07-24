// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.CancellationToken;
import dev.slsk.messaging.messages.OutgoingMessage;
import dev.slsk.network.tcp.IConnection;
import java.util.concurrent.CompletableFuture;

/** Provides framed client connections to the Soulseek network. */
public interface IMessageConnection extends IConnection {
    void addMessageDataReadListener(MessageConnectionEventListener<MessageDataEventArgs> listener);

    void removeMessageDataReadListener(MessageConnectionEventListener<MessageDataEventArgs> listener);

    void addMessageReadListener(MessageConnectionEventListener<MessageEventArgs> listener);

    void removeMessageReadListener(MessageConnectionEventListener<MessageEventArgs> listener);

    void addMessageReceivedListener(MessageConnectionEventListener<MessageReceivedEventArgs> listener);

    void removeMessageReceivedListener(MessageConnectionEventListener<MessageReceivedEventArgs> listener);

    void addMessageWrittenListener(MessageConnectionEventListener<MessageEventArgs> listener);

    void removeMessageWrittenListener(MessageConnectionEventListener<MessageEventArgs> listener);

    /** Returns the received message-code width. */
    int getCodeLength();

    /** Returns whether this is a server rather than peer connection. */
    boolean isServerConnection();

    /** Returns whether the continuous read loop is running. */
    boolean isReadingContinuously();

    /** Returns the associated peer username, or an empty string. */
    String getUsername();

    /** Starts the internal continuous read loop if necessary. */
    void startReadingContinuously();

    /** Writes an outgoing message. */
    CompletableFuture<Void> writeAsync(OutgoingMessage message, CancellationToken cancellationToken);

    /** Writes an outgoing message without a cancellable token. */
    default CompletableFuture<Void> writeAsync(OutgoingMessage message) {
        return writeAsync(message, CancellationToken.none());
    }
}
