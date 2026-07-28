// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import dev.slsk.Username;
import java.time.Instant;
import java.util.Objects;

/**
 * Private messages.
 *
 * <p>There is exactly one event here, and no way to read message history from
 * the library, because a private message is history the moment it arrives. The
 * library holds what is true now; an append-only record of things that happened
 * belongs to the application, which is the only party that knows how long to
 * keep it and where.
 */
public sealed interface ChatEvent extends SoulseekEvent {

    /**
     * Somebody sent us a message.
     *
     * <p>The library acknowledges the message to the server once this has been
     * delivered to at least one listener that did not throw. If no listener is
     * registered, or every listener throws, it is not acknowledged and the
     * server delivers it again at the next login — which is the behaviour a
     * consumer wants when its own handler is broken, and the one place a
     * listener's exception is observed rather than merely reported.
     *
     * @param from who sent it
     * @param message what they said
     * @param wasReplayed whether the server is redelivering something sent while
     *     we were offline, which is what a UI needs to decide whether to notify
     * @param sentAt when the sender sent it
     * @param at when we received it
     */
    record MessageReceived(Username from, String message, boolean wasReplayed, Instant sentAt, Instant at)
            implements ChatEvent {

        public MessageReceived {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(message, "message");
            Objects.requireNonNull(sentAt, "sentAt");
            Objects.requireNonNull(at, "at");
        }
    }
}
