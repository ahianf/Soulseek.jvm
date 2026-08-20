// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import dev.slsk.Subscription;
import dev.slsk.internal.diagnostics.DiagnosticSource;
import java.util.function.Consumer;

/** Responds to incoming search requests. */
public interface SearchResponder extends DiagnosticSource {
    enum Kind {
        REQUEST_RECEIVED,
        RESPONSE_DELIVERED,
        RESPONSE_DELIVERY_FAILED
    }

    <T> Subscription subscribe(Kind kind, Consumer<? super T> listener);

    boolean tryDiscard(int responseToken);

    /**
     * Answers a peer's search, blocking until the response is delivered.
     *
     * <p>This asks the {@link dev.slsk.spi.ShareCatalog}, connects to the
     * peer and writes to it, so every caller on a read loop dispatches it. That
     * is what keeps a slow catalog from stalling one peer's traffic, and it is
     * the caller's decision rather than this one's.
     *
     * @param username the peer that searched
     * @param token the peer's search token
     * @param query the query
     * @return whether a response was delivered
     */
    boolean tryRespond(String username, int token, String query);

    /**
     * Delivers a response that was cached when the peer could not be reached.
     *
     * @param responseToken the solicitation token the response was cached under
     * @return whether the cached response was delivered
     */
    boolean tryRespond(int responseToken);
}
