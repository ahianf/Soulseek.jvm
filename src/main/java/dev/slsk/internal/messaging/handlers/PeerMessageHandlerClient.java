// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.search.SearchInternal;
import dev.slsk.internal.transfer.TransferInternal;
import java.util.Map;

/** Internal client state consumed by the peer message handler. */
public interface PeerMessageHandlerClient {
    SoulseekClientOptions getOptions();

    Waiter getWaiter();

    Map<Integer, SearchInternal> getSearches();

    Map<Integer, TransferInternal> getDownloadDictionary();

    /**
     * Returns what peers are served from.
     *
     * @return the installed share catalog, never {@code null}
     */
    dev.slsk.spi.ShareCatalog getShareCatalog();

    /**
     * Returns our own logged-in username, which a peer keys our search response
     * on.
     *
     * @return the logged-in username, or {@code null}
     */
    String getLoggedInUsername();

    /**
     * Returns what peers are told about this account.
     *
     * @return the profile, never {@code null}
     */
    dev.slsk.UserProfile getProfile();
}
