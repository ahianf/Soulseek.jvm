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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
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
 * <p><strong>Delivery is not on the publishing thread.</strong> One virtual
 * thread per bus takes from a bounded queue and runs the listeners, because the
 * publishing thread is a network read loop: a private message arriving on the
 * server connection used to run the consumer's listener and then a full
 * acknowledgement round trip before the loop could read the next protocol
 * message, so one slow listener stalled chat, room messages, user status and
 * ticker updates alike. That is not listener misbehaviour — writing to a
 * database on a chat message is the obviously reasonable thing to do — and the
 * library is the wrong place for the cost to land.
 *
 * <p>Ordering is guaranteed per bus, not across buses: one queue and one thread
 * per bus is what guarantees it. <strong>An overflowing queue blocks the
 * publisher rather than dropping.</strong> A dropped event is a lost chat
 * message or a lost terminal state; blocking degrades, under sustained
 * overload only, to exactly the behaviour this replaces, and is therefore never
 * worse than what was there.
 *
 * <p>The clean-delivery count that the private-message acknowledgement depends
 * on is still exact. It cannot be returned to a publisher that has already
 * moved on, so {@link #publish(Object, IntConsumer)} takes a continuation and
 * runs it on the delivery thread once every listener has been given the event.
 *
 * @param <T> the facet's event type
 */
public final class EventBus<T> implements EventStream<T>, AutoCloseable {

    /**
     * How many events may be waiting before a publisher blocks.
     *
     * <p>Large enough that no burst a read loop can produce reaches it, small
     * enough that a listener wedged forever cannot grow the queue without
     * bound. The number is a backstop, not a tuning knob: the design decision
     * is that reaching it blocks.
     */
    private static final int CAPACITY = 1024;

    /** How long {@link #close()} waits for a listener already running. */
    private static final long CLOSE_TIMEOUT_MILLIS = 2_000;

    private final String name;
    private final DiagnosticSink diagnostics;

    /** Guards the listener list, and pairs it with facet state changes. */
    private final Object gate = new Object();

    private final List<Registration<T>> registrations = new ArrayList<>();

    private final BlockingQueue<Delivery<T>> pending = new ArrayBlockingQueue<>(CAPACITY);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread deliveryThread;

    /**
     * Creates a bus.
     *
     * @param name the facet name, used in diagnostic messages and the delivery
     *     thread's name
     * @param diagnostics where a throwing listener is reported
     */
    public EventBus(String name, DiagnosticSink diagnostics) {
        this.name = Objects.requireNonNull(name, "name");
        this.diagnostics = Objects.requireNonNull(diagnostics, "diagnostics");
        this.deliveryThread = Thread.ofVirtual().name("soulseek-events-" + name).start(this::deliverContinuously);
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
     */
    public void publish(T event) {
        publish(event, null);
    }

    /**
     * Publishes an event and runs {@code continuation} once it has been
     * delivered.
     *
     * <p>The continuation receives how many listeners accepted the event
     * without throwing, and runs on the delivery thread — so whatever it does
     * next is off the read loop too. That is what the private-message
     * acknowledgement needs: the rule is unchanged, acknowledge if and only if
     * at least one listener took the message cleanly, but the round trip that
     * carries it no longer sits between two protocol messages.
     *
     * @param event the event
     * @param continuation what to do with the clean-delivery count, or
     *     {@code null}
     */
    public void publish(T event, IntConsumer continuation) {
        Objects.requireNonNull(event, "event");
        enqueue(new Delivery<>(event, capture(), continuation));
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
     */
    public void mutateAndPublish(Supplier<T> change) {
        Objects.requireNonNull(change, "change");
        T event;
        List<Registration<T>> targets;
        synchronized (gate) {
            event = change.get();
            if (event == null) {
                return;
            }
            targets = new ArrayList<>(registrations);
        }
        enqueue(new Delivery<>(event, targets, null));
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

    /**
     * Stops the delivery thread.
     *
     * <p>Whatever is still queued is dropped, deliberately: the client is going
     * away, and a consumer that has asked for that is not waiting to be told
     * about events from before it asked. A listener already running is given
     * {@link #CLOSE_TIMEOUT_MILLIS} to finish, because it is consumer code and
     * nothing here can make it stop.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        clear();
        deliveryThread.interrupt();
        try {
            deliveryThread.join(CLOSE_TIMEOUT_MILLIS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Drops every listener. Called when the client closes. */
    public void clear() {
        synchronized (gate) {
            registrations.clear();
        }
    }

    /**
     * Hands one delivery to the delivery thread, blocking if it is behind.
     *
     * <p>Uninterruptibly: an interrupt belongs to whatever the publisher does
     * next, and a read loop that is interrupted mid-publish must still not lose
     * the event it was carrying. The wait re-checks {@link #closed}, because
     * after close nothing consumes the queue — a publisher that kept blocking
     * on a full one would be wedged forever, and dropping is exactly what
     * {@link #close} already documents for everything still queued.
     *
     * <p>A publish from the delivery thread itself — a listener or a
     * continuation calling back into a facet — must never block on this queue:
     * it is the queue's only consumer, so a full queue would deadlock the bus
     * permanently and wedge every publisher behind it, read loops included.
     * When that publish cannot be queued it is delivered inline. Ordering
     * degrades for that one event, only on a bus already at capacity; the
     * alternative is not degraded ordering but no delivery ever again.
     */
    private void enqueue(Delivery<T> delivery) {
        if (closed.get()) {
            return;
        }
        if (Thread.currentThread() == deliveryThread) {
            if (!pending.offer(delivery)) {
                deliver(delivery);
            }
            return;
        }
        boolean interrupted = false;
        try {
            while (!closed.get()) {
                try {
                    if (pending.offer(delivery, 100, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                        return;
                    }
                } catch (InterruptedException exception) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** The delivery thread: take one, deliver it, tell whoever asked. */
    private void deliverContinuously() {
        while (true) {
            Delivery<T> delivery;
            try {
                delivery = pending.take();
            } catch (InterruptedException interrupted) {
                return;
            }
            deliver(delivery);
        }
    }

    /** Delivers one event and runs its continuation, containing both. */
    private void deliver(Delivery<T> delivery) {
        int delivered = dispatch(delivery.event(), delivery.targets());
        IntConsumer continuation = delivery.continuation();
        if (continuation == null) {
            return;
        }
        try {
            continuation.accept(delivered);
        } catch (RuntimeException | Error exception) {
            diagnostics.warning(
                    "The continuation for a " + name + " event threw "
                            + exception.getClass().getName() + "; it was contained",
                    exception);
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

    /**
     * One event, the listeners it was captured against, and what to do after.
     *
     * <p>The listener list travels with the event rather than being read at
     * delivery time. That is what preserves {@link #attach}'s atomicity now
     * that delivery is later than publication: a listener that attached after
     * the capture is not in it, and its own snapshot already includes whatever
     * this event describes.
     */
    private record Delivery<T>(T event, List<Registration<T>> targets, IntConsumer continuation) {}
}
