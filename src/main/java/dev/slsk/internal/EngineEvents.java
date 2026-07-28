// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.Subscription;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * What the engine tells the facets, keyed by kind.
 *
 * <p>This replaces {@code ClientEventSupport}: forty-seven pairs of named
 * {@code addXListener} / {@code removeXListener} methods, five hundred lines of
 * them, written by hand because the public client interface had to declare each
 * one. That interface is gone, and with it the reason. A kind and a {@link
 * Consumer} say the same thing in one method.
 *
 * <p>Two behaviours are not just plumbing:
 *
 * <p><strong>Dispatch is contained.</strong> These events are raised from
 * message handlers running on read loops. A listener that throws used to
 * propagate straight back into the loop it came from and take the connection
 * with it. Now the fault is handed to the engine's diagnostic sink and the
 * remaining listeners still run. This is the same rule the public {@code
 * EventBus} applies to consumer listeners, and for the same reason; it applies
 * here because a facet's own translation code is not infallible either.
 *
 * <p><strong>Diagnostic faults are swallowed rather than reported.</strong>
 * There is nowhere to report the failure of a diagnostic listener except the
 * diagnostic channel, which is the thing that just failed.
 */
final class EngineEvents {

    /** Everything the engine raises. */
    enum Kind {
        BROWSE_PROGRESS_UPDATED,
        CONNECTED,
        DEMOTED_FROM_DISTRIBUTED_BRANCH_ROOT,
        DIAGNOSTIC_GENERATED,
        DISCONNECTED,
        DISTRIBUTED_CHILD_ADDED,
        DISTRIBUTED_CHILD_DISCONNECTED,
        DISTRIBUTED_NETWORK_RESET,
        DISTRIBUTED_NETWORK_STATE_CHANGED,
        DISTRIBUTED_PARENT_ADOPTED,
        DISTRIBUTED_PARENT_DISCONNECTED,
        DOWNLOAD_DENIED,
        DOWNLOAD_FAILED,
        EXCLUDED_SEARCH_PHRASES_RECEIVED,
        GLOBAL_MESSAGE_RECEIVED,
        KICKED_FROM_SERVER,
        LOGGED_IN,
        PRIVATE_MESSAGE_RECEIVED,
        PRIVATE_ROOM_MEMBERSHIP_ADDED,
        PRIVATE_ROOM_MEMBERSHIP_REMOVED,
        PRIVATE_ROOM_MODERATED_USER_LIST_RECEIVED,
        PRIVATE_ROOM_MODERATION_ADDED,
        PRIVATE_ROOM_MODERATION_REMOVED,
        PRIVATE_ROOM_USER_LIST_RECEIVED,
        PRIVILEGED_USER_LIST_RECEIVED,
        PRIVILEGE_NOTIFICATION_RECEIVED,
        PROMOTED_TO_DISTRIBUTED_BRANCH_ROOT,
        PUBLIC_CHAT_MESSAGE_RECEIVED,
        ROOM_JOINED,
        ROOM_LEFT,
        ROOM_LIST_RECEIVED,
        ROOM_MESSAGE_RECEIVED,
        ROOM_TICKER_ADDED,
        ROOM_TICKER_LIST_RECEIVED,
        ROOM_TICKER_REMOVED,
        SEARCH_REQUEST_RECEIVED,
        SEARCH_RESPONSE_DELIVERED,
        SEARCH_RESPONSE_DELIVERY_FAILED,
        SEARCH_RESPONSE_RECEIVED,
        SEARCH_STATE_CHANGED,
        SERVER_INFO_RECEIVED,
        STATE_CHANGED,
        TRANSFER_PROGRESS_UPDATED,
        TRANSFER_STATE_CHANGED,
        USER_CANNOT_CONNECT,
        USER_STATISTICS_CHANGED,
        USER_STATUS_CHANGED
    }

    private final Map<Kind, CopyOnWriteArrayList<Consumer<?>>> listeners = new EnumMap<>(Kind.class);

    /**
     * Where a contained fault goes. Supplied rather than read from a field
     * because the engine's diagnostic sink raises {@link
     * Kind#DIAGNOSTIC_GENERATED} through this object, so it cannot exist before
     * it.
     */
    private final BiConsumer<Kind, Throwable> onListenerFault;

    EngineEvents(BiConsumer<Kind, Throwable> onListenerFault) {
        this.onListenerFault = Objects.requireNonNull(onListenerFault, "onListenerFault");
        for (Kind kind : Kind.values()) {
            listeners.put(kind, new CopyOnWriteArrayList<>());
        }
    }

    /**
     * Registers a listener for one kind.
     *
     * @param kind which event
     * @param listener receives the payload
     * @param <T> the payload type
     * @return a subscription that removes the listener; idempotent
     */
    <T> Subscription on(Kind kind, Consumer<? super T> listener) {
        CopyOnWriteArrayList<Consumer<?>> registered = listeners.get(kind);
        Objects.requireNonNull(listener, "listener");
        registered.add(listener);
        return new Subscription() {
            private boolean closed;

            @Override
            public void close() {
                if (!closed) {
                    closed = true;
                    registered.remove(listener);
                }
            }
        };
    }

    /**
     * Raises an event, containing any listener fault.
     *
     * @param kind which event
     * @param payload the payload, which is {@code null} for the kinds that
     *     carry none
     * @param <T> the payload type
     */
    @SuppressWarnings("unchecked")
    <T> void raise(Kind kind, T payload) {
        for (Consumer<?> listener : listeners.get(kind)) {
            try {
                ((Consumer<T>) listener).accept(payload);
            } catch (Throwable failure) {
                if (kind != Kind.DIAGNOSTIC_GENERATED) {
                    onListenerFault.accept(kind, failure);
                }
            }
        }
    }

    /**
     * Returns how many listeners a kind has. For tests, which otherwise cannot
     * tell registration from silence.
     *
     * @param kind which event
     * @return the listener count
     */
    int count(Kind kind) {
        return listeners.get(kind).size();
    }
}
