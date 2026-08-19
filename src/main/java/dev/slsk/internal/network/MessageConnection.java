// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.network.tcp.Connection;

/** Provides framed client connections to the Soulseek network. */
public interface MessageConnection extends Connection {
    void addMessageDataReadListener(MessageConnectionEventListener<MessageDataEvent> listener);

    void removeMessageDataReadListener(MessageConnectionEventListener<MessageDataEvent> listener);

    void addMessageReadListener(MessageConnectionEventListener<MessageEvent> listener);

    void removeMessageReadListener(MessageConnectionEventListener<MessageEvent> listener);

    void addMessageReceivedListener(MessageConnectionEventListener<MessageReceivedEvent> listener);

    void removeMessageReceivedListener(MessageConnectionEventListener<MessageReceivedEvent> listener);

    void addMessageWrittenListener(MessageConnectionEventListener<MessageEvent> listener);

    void removeMessageWrittenListener(MessageConnectionEventListener<MessageEvent> listener);

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

    /** Writes an outgoing message, blocking until it lands. */
    void write(OutgoingMessage message, CancellationSignal cancellationSignal)
            throws InterruptedException, java.util.concurrent.TimeoutException;

    /** Writes an outgoing message without a cancellable token. */
    default void write(OutgoingMessage message) throws InterruptedException, java.util.concurrent.TimeoutException {
        write(message, CancellationSignal.none());
    }
}
