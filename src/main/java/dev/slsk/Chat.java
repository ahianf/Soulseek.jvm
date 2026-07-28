// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.events.ChatEvent;

/**
 * Private messages between users.
 *
 * <p>Two members, because there is very little to this: send one, and receive
 * them. There is no scrollback here and no way to ask for it. A message is
 * history the instant it arrives, and history belongs to the application, which
 * is the only party that knows how much of it to keep and where to put it.
 *
 * <p>Acknowledgement is automatic and is not a method a consumer calls. The
 * library acknowledges a message to the server once it has been delivered to a
 * listener that did not throw; if nobody is listening, or every listener throws,
 * it stays unacknowledged and the server sends it again next login.
 */
public interface Chat {

    /**
     * Sends a private message.
     *
     * @param to the recipient
     * @param message what to say
     * @param signal cancels the send
     */
    void send(Username to, String message, CancellationSignal signal);

    /**
     * Returns the stream of chat events.
     *
     * @return the event stream
     */
    EventStream<ChatEvent> events();
}
