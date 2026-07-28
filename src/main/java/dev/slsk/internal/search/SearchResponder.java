// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import dev.slsk.internal.diagnostics.DiagnosticSource;
import dev.slsk.internal.events.SearchRequestEvent;
import dev.slsk.internal.events.SearchRequestResponseEvent;
import java.util.concurrent.CompletableFuture;

/** Responds to incoming search requests. */
public interface SearchResponder extends DiagnosticSource {
    void addRequestReceivedListener(SearchResponderEventListener<SearchRequestEvent> listener);

    void removeRequestReceivedListener(SearchResponderEventListener<SearchRequestEvent> listener);

    void addResponseDeliveredListener(SearchResponderEventListener<SearchRequestResponseEvent> listener);

    void removeResponseDeliveredListener(SearchResponderEventListener<SearchRequestResponseEvent> listener);

    void addResponseDeliveryFailedListener(SearchResponderEventListener<SearchRequestResponseEvent> listener);

    void removeResponseDeliveryFailedListener(SearchResponderEventListener<SearchRequestResponseEvent> listener);

    boolean tryDiscard(int responseToken);

    CompletableFuture<Boolean> tryRespondAsync(String username, int token, String query);

    CompletableFuture<Boolean> tryRespondAsync(int responseToken);
}
