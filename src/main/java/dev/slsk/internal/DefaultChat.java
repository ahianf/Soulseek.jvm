// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.CancellationSignal;
import dev.slsk.Chat;
import dev.slsk.EventStream;
import dev.slsk.Username;
import dev.slsk.events.ChatEvent;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.events.PrivateMessageReceivedEvent;
import java.time.Instant;
import java.util.Objects;

/**
 * {@link Chat}, over the engine.
 *
 * <p>This is where acknowledge-on-dispatch lives. The old surface exposed
 * {@code acknowledgePrivateMessage(id)} and left the consumer to call it, which
 * gets the failure mode exactly backwards: a consumer that forgets acknowledges
 * nothing and is redelivered everything forever, and one that acknowledges
 * eagerly in its listener loses the message if its own handling then fails.
 *
 * <p>The library acknowledges when, and only when, the message has reached a
 * listener that did not throw. {@link EventBus#publish} returns that count
 * precisely so this decision can be made. If nothing is listening, or everything
 * listening threw, the message is left unacknowledged and the server redelivers
 * it at the next login — which is what a consumer with a broken handler actually
 * wants, and it is the one place a listener's exception is observed rather than
 * merely reported.
 */
final class DefaultChat implements Chat {

    private final ServerLink server;
    private final EventBus<ChatEvent> events;
    private final DiagnosticSink diagnostics;

    DefaultChat(SoulseekEngine client, EventBus<ChatEvent> events, DiagnosticSink diagnostics) {
        this.server = Objects.requireNonNull(client, "client").server();
        this.events = Objects.requireNonNull(events, "events");
        this.diagnostics = Objects.requireNonNull(diagnostics, "diagnostics");
        client.events().on(Kind.PRIVATE_MESSAGE_RECEIVED, (PrivateMessageReceivedEvent event) -> onMessage(event));
    }

    private void onMessage(PrivateMessageReceivedEvent event) {
        if (event == null) {
            return;
        }
        int delivered = events.publish(new ChatEvent.MessageReceived(
                Username.of(event.getUsername()),
                event.getMessage(),
                event.isReplayed(),
                event.getTimestamp(),
                Instant.now()));

        if (delivered == 0) {
            diagnostics.warning("Private message " + event.getId() + " from " + event.getUsername()
                    + " reached no listener cleanly and was not acknowledged; "
                    + "the server will deliver it again at the next login");
            return;
        }
        try {
            server.acknowledgePrivateMessage(event.getId());
        } catch (RuntimeException exception) {
            diagnostics.warning("Failed to acknowledge private message " + event.getId(), exception);
        }
    }

    @Override
    public void send(Username to, String message, CancellationSignal signal) {
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(signal, "signal");
        server.sendPrivateMessage(to.value(), message, signal);
    }

    @Override
    public EventStream<ChatEvent> events() {
        return events;
    }
}
