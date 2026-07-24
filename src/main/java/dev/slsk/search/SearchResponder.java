// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.search;

import dev.slsk.diagnostics.IDiagnosticGenerator;
import dev.slsk.eventargs.SearchRequestEventArgs;
import dev.slsk.eventargs.SearchRequestResponseEventArgs;
import java.util.concurrent.CompletableFuture;

/** Responds to incoming search requests. */
public interface SearchResponder extends IDiagnosticGenerator {
    void addRequestReceivedListener(SearchResponderEventListener<SearchRequestEventArgs> listener);

    void removeRequestReceivedListener(SearchResponderEventListener<SearchRequestEventArgs> listener);

    void addResponseDeliveredListener(SearchResponderEventListener<SearchRequestResponseEventArgs> listener);

    void removeResponseDeliveredListener(SearchResponderEventListener<SearchRequestResponseEventArgs> listener);

    void addResponseDeliveryFailedListener(SearchResponderEventListener<SearchRequestResponseEventArgs> listener);

    void removeResponseDeliveryFailedListener(SearchResponderEventListener<SearchRequestResponseEventArgs> listener);

    boolean tryDiscard(int responseToken);

    CompletableFuture<Boolean> tryRespondAsync(String username, int token, String query);

    CompletableFuture<Boolean> tryRespondAsync(int responseToken);
}
