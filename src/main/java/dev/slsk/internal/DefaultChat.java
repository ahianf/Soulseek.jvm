// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.Chat;
import dev.slsk.EventStream;
import dev.slsk.events.ChatEvent;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.common.Usernames;
import dev.slsk.internal.concurrent.BlockingInvocation;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.events.EventBus;
import dev.slsk.internal.events.PrivateMessageReceivedEvent;
import dev.slsk.user.Username;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

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
    private final SoulseekEngine client;
    private final EventBus<ChatEvent> events;
    private final DiagnosticSink diagnostics;

    DefaultChat(SoulseekEngine client, EventBus<ChatEvent> events, DiagnosticSink diagnostics) {
        this.client = Objects.requireNonNull(client, "client");
        this.server = client.server();
        this.events = Objects.requireNonNull(events, "events");
        this.diagnostics = Objects.requireNonNull(diagnostics, "diagnostics");
        client.events().on(Kind.PRIVATE_MESSAGE_RECEIVED, (PrivateMessageReceivedEvent event) -> onMessage(event));
    }

    private void onMessage(PrivateMessageReceivedEvent event) {
        if (event == null) {
            return;
        }
        Username sender = Usernames.fromWire(event.getUsername());
        if (sender == null) {
            // Unrepresentable sender. Throwing here used to leave the message
            // both undelivered and unacknowledged, so the server redelivered it
            // at every login forever; skipping keeps the redelivery but names it.
            diagnostics.warning("Private message " + event.getId()
                    + " carries a sender no username can represent and was not delivered");
            return;
        }
        // The acknowledgement is a continuation rather than the next statement,
        // and the rule it encodes is unchanged. What changes is where it runs:
        // on the bus's delivery thread, so a listener and a full server round
        // trip no longer sit between the read loop and the next message.
        events.publish(
                new ChatEvent.MessageReceived(
                        sender, event.getMessage(), event.isReplayed(), event.getTimestamp(), Instant.now()),
                delivered -> acknowledge(event, delivered));
    }

    /**
     * Acknowledges a private message if, and only if, a listener took it
     * cleanly.
     *
     * @param event the message
     * @param delivered how many listeners accepted it without throwing
     */
    private void acknowledge(PrivateMessageReceivedEvent event, int delivered) {
        if (delivered == 0) {
            diagnostics.warning("Private message " + event.getId() + " from " + event.getUsername()
                    + " reached no listener cleanly and was not acknowledged; "
                    + "the server will deliver it again at the next login");
            return;
        }
        try {
            server.acknowledgePrivateMessage(event.getId());
        } catch (InterruptedException interrupted) {
            // The bus's delivery thread was asked to stop; the message stays
            // unacknowledged and the server redelivers it at the next login.
            Thread.currentThread().interrupt();
            diagnostics.warning("Interrupted acknowledging private message " + event.getId(), interrupted);
        } catch (RuntimeException exception) {
            diagnostics.warning("Failed to acknowledge private message " + event.getId(), exception);
        }
    }

    @Override
    public void send(Username to, String message) throws InterruptedException {
        BlockingInvocation.run(signal -> {
            send(to, message, signal);
            return null;
        });
    }

    @Override
    public void send(Username to, String message, Duration timeout) throws InterruptedException, TimeoutException {
        BlockingInvocation.run(client.getScheduler(), timeout, signal -> {
            send(to, message, signal);
            return null;
        });
    }

    private void send(Username to, String message, dev.slsk.internal.concurrent.CancellationSignal signal)
            throws InterruptedException {
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(message, "message");
        server.sendPrivateMessage(to.value(), message, signal);
    }

    @Override
    public EventStream<ChatEvent> events() {
        return events;
    }
}
