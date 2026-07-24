// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.handlers;

import dev.slsk.common.IWaiter;
import dev.slsk.options.SoulseekClientOptions;
import dev.slsk.search.SearchInternal;
import dev.slsk.transfer.TransferInternal;
import java.util.Map;

/** Internal client state consumed by the peer message handler. */
public interface PeerMessageHandlerClient {
    SoulseekClientOptions getOptions();

    IWaiter getWaiter();

    Map<Integer, SearchInternal> getSearches();

    Map<Integer, TransferInternal> getDownloadDictionary();
}
