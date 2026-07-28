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

    /**
     * Returns who we serve and in what order.
     *
     * @return the upload policy, never {@code null}
     */
    dev.slsk.spi.UploadPolicy getUploadPolicy();

    /**
     * Returns what admits or refuses a peer's request.
     *
     * @return the admission
     */
    dev.slsk.internal.UploadAdmission getUploadAdmission();

    /**
     * Serves a file to a peer whose request the policy allowed.
     *
     * @param user who asked
     * @param path the file they asked for
     */
    void serveUpload(dev.slsk.Username user, String path);

    /**
     * What a peer's unsolicited offer of a file turned out to be.
     *
     * <p>Three outcomes because the peer can act on the difference: an offer we
     * took up gets no reply here (the transfer answers it), one for a file we
     * already have is {@code Complete}, and anything else is {@code Cancelled}.
     */
    enum OfferDisposition {
        /** Ours, and now starting. The download writes the acceptance itself. */
        TAKEN,
        /** Ours, but already finished. */
        COMPLETE,
        /** Not something we asked for. */
        UNKNOWN
    }

    /**
     * Offers a file the peer says it is ready to send.
     *
     * <p>A peer sends this when our place in <em>its</em> queue comes up, which
     * may be hours after we asked. By then the download is very often sitting in
     * our own queue waiting for a slot or serving out a retry backoff, and the
     * engine has never heard of it — so matching on the engine's live transfers
     * alone answers "no" to the one moment that mattered, and the reply throws
     * away a queue position that took those hours to earn.
     *
     * <p>Taking the offer starts the download immediately, past the concurrency
     * caps. Those caps exist to stop us opening connections at a peer; a peer
     * volunteering a file is the opposite situation, and both reference clients
     * accept here.
     *
     * @param username who is offering
     * @param filename what they are offering
     * @param offer their request, carrying the token and size the transfer needs
     * @return what became of the offer
     */
    OfferDisposition offerDownload(
            String username, String filename, dev.slsk.internal.messaging.messages.TransferRequest offer);
}
