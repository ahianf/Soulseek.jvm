// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.slsk.Subscription;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class SubscriptionsTest {
    @Test
    void closeRemovesTheListenerAndIsIdempotent() {
        CopyOnWriteArrayList<Consumer<String>> listeners = new CopyOnWriteArrayList<>();
        Consumer<String> listener = ignored -> {};

        Subscription subscription = Subscriptions.add(listeners, listener);

        assertEquals(1, listeners.size());
        subscription.close();
        subscription.close();
        assertEquals(0, listeners.size());
    }
}
