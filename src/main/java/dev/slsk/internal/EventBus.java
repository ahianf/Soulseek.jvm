// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.Attachment;
import dev.slsk.EventStream;
import dev.slsk.Subscription;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The one implementation of {@link EventStream}. One per facet.
 *
 * <p>Two things here are load-bearing and neither is visible from the interface.
 *
 * <p><strong>Containment.</strong> {@link #dispatch} invokes every listener
 * inside a {@code try}, reports a throw to the diagnostic sink, and carries on
 * to the next. The dispatch this replaces did not: it iterated the listener list
 * and called straight through, so the first consumer to throw unwound the stack
 * of whatever raised the event — a message handler, or a connection read loop. A
 * rendering bug in a consumer could drop the connection. Closing that is why
 * this class exists.
 *
 * <p><strong>Atomicity.</strong> {@link #attach} must pair a state snapshot with
 * a subscription so that no event falls between them and none arrives twice.
 * That requires more than locking the snapshot against the listener list: the
 * <em>state change</em> and the event describing it have to be one step too,
 * because otherwise a change can land after the snapshot is taken and its event
 * before the listener is registered. So the facet does not mutate its state and
 * then call {@code publish}; it calls {@link #mutateAndPublish}, and this class
 * owns the ordering. Everything a facet needs to be atomic happens under {@link
 * #gate}:
 *
 * <ul>
 *   <li>{@link #attach} — take the snapshot, register the listener
 *   <li>{@link #mutateAndPublish} — apply the change, capture the listener list
 * </ul>
 *
 * <p>Delivery itself deliberately happens <em>outside</em> the lock, against the
 * list captured inside it. No library lock is ever held across a consumer
 * callback, which is what stops a slow or reentrant listener from deadlocking
 * the client. The captured list is what makes that safe: a listener registered
 * after the capture is not in it, and its subscription began after the change
 * was already visible to a snapshot.
 *
 * <p>Ordering is guaranteed per bus, not across buses. Delivery is synchronous
 * on the publishing thread, which matches the behaviour being replaced and is
 * what makes the clean-delivery count meaningful — the private-message
 * acknowledgement depends on it.
 *
 * @param <T> the facet's event type
 */
public final class EventBus<T> implements EventStream<T> {

    private final String name;
    private final DiagnosticSink diagnostics;

    /** Guards the listener list, and pairs it with facet state changes. */
    private final Object gate = new Object();

    private final List<Registration<T>> registrations = new ArrayList<>();

    /**
     * Creates a bus.
     *
     * @param name the facet name, used only in diagnostic messages
     * @param diagnostics where a throwing listener is reported
     */
    public EventBus(String name, DiagnosticSink diagnostics) {
        this.name = Objects.requireNonNull(name, "name");
        this.diagnostics = Objects.requireNonNull(diagnostics, "diagnostics");
    }

    @Override
    public Subscription subscribe(Consumer<? super T> listener) {
        Objects.requireNonNull(listener, "listener");
        return register(new Registration<>(null, listener));
    }

    @Override
    public <U extends T> Subscription subscribe(Class<U> type, Consumer<? super U> listener) {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(listener, "listener");
        @SuppressWarnings("unchecked")
        Consumer<? super T> erased = (Consumer<? super T>) listener;
        return register(new Registration<>(type, erased));
    }

    /**
     * Captures {@code snapshot} and registers {@code listener} as one step, so
     * the stream begins exactly where the snapshot ends.
     *
     * @param snapshot supplies the state; invoked once, under the lock
     * @param listener the listener
     * @param <S> the snapshot type
     * @return the state and the subscription
     */
    public <S> Attachment<S> attach(Supplier<S> snapshot, Consumer<? super T> listener) {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(listener, "listener");
        synchronized (gate) {
            Subscription subscription = register(new Registration<>(null, listener));
            return new Attachment<>(snapshot.get(), subscription);
        }
    }

    /**
     * Publishes an event that describes no state this facet holds.
     *
     * <p>Use this only when there is nothing for a snapshot to disagree with —
     * an inbound chat message, a search request from a peer. Anything that
     * changes what a snapshot would report must go through {@link
     * #mutateAndPublish} instead, or {@link #attach} has a race.
     *
     * @param event the event
     * @return how many listeners accepted it without throwing
     */
    public int publish(T event) {
        Objects.requireNonNull(event, "event");
        return dispatch(event, capture());
    }

    /**
     * Applies a state change and publishes the event describing it, as one step
     * with respect to {@link #attach}.
     *
     * <p>{@code change} may return {@code null} to mean "nothing actually
     * changed, publish nothing". That is not a nicety: the underlying client
     * raises connected, logged-in and state-changed for what is a single
     * transition, and a consumer should see one event rather than three. The
     * decision has to be taken under the lock, because it depends on comparing
     * against the last state published.
     *
     * @param change applies the change and returns the event describing it, or
     *     {@code null} to publish nothing. Invoked once, under the lock. It must
     *     not block, and must not call back into this bus.
     * @return how many listeners accepted the event without throwing; zero if
     *     nothing was published
     */
    public int mutateAndPublish(Supplier<T> change) {
        Objects.requireNonNull(change, "change");
        T event;
        List<Registration<T>> targets;
        synchronized (gate) {
            event = change.get();
            if (event == null) {
                return 0;
            }
            targets = new ArrayList<>(registrations);
        }
        return dispatch(event, targets);
    }

    /**
     * Runs a state change under the same lock that {@link #attach} uses, without
     * publishing anything. For changes a consumer cannot observe as an event but
     * a snapshot would report.
     *
     * @param change the change
     */
    public void mutate(Runnable change) {
        Objects.requireNonNull(change, "change");
        synchronized (gate) {
            change.run();
        }
    }

    /**
     * Returns how many listeners are registered. For tests and diagnostics; a
     * leaked subscription shows up here.
     *
     * @return the listener count
     */
    public int listenerCount() {
        synchronized (gate) {
            return registrations.size();
        }
    }

    /** Drops every listener. Called when the client closes. */
    public void clear() {
        synchronized (gate) {
            registrations.clear();
        }
    }

    private List<Registration<T>> capture() {
        synchronized (gate) {
            return new ArrayList<>(registrations);
        }
    }

    /** Delivers to the captured listeners, containing anything they throw. */
    private int dispatch(T event, List<Registration<T>> targets) {
        int delivered = 0;
        for (Registration<T> registration : targets) {
            if (!registration.wants(event)) {
                continue;
            }
            try {
                registration.listener().accept(event);
                delivered++;
            } catch (RuntimeException | Error exception) {
                diagnostics.warning(
                        "A " + name + " event listener threw "
                                + exception.getClass().getName()
                                + " handling " + event.getClass().getSimpleName()
                                + "; it was contained and the remaining listeners still ran",
                        exception);
            }
        }
        return delivered;
    }

    private Subscription register(Registration<T> registration) {
        synchronized (gate) {
            registrations.add(registration);
        }
        AtomicBoolean closed = new AtomicBoolean();
        return () -> {
            if (closed.compareAndSet(false, true)) {
                synchronized (gate) {
                    registrations.remove(registration);
                }
            }
        };
    }

    /**
     * One listener and the concrete type it asked for; a {@code null} type means
     * every event on the stream. Identity equality is what {@link
     * java.util.List#remove} needs, and a record over a lambda gives exactly
     * that, since two lambdas are never equal.
     */
    private record Registration<T>(Class<?> type, Consumer<? super T> listener) {

        boolean wants(T event) {
            return type == null || type.isInstance(event);
        }
    }
}
