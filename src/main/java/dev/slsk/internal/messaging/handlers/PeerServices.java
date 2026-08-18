// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

/**
 * What this client offers a peer.
 *
 * <p>Everything a peer's message can ask of us that is not the protocol: what
 * is shared, what this account says about itself, who may upload and in what
 * order, and whether an offered file is one we asked for. The plumbing a
 * handler needs — options, the correlator, the two registries — is not here,
 * because that is not what a peer is asking about.
 *
 * <p>The engine answers this, and it is the last of its collaborator interfaces
 * to do so. The state behind every member is the upload, share and profile
 * state {@code TransferDomain} takes in Phase 4; this is where it will be
 * plugged in when it does.
 */
public interface PeerServices {

    /** Returns what peers are served from. */
    dev.slsk.spi.ShareCatalog catalog();

    /** Returns what peers are told about this account. */
    dev.slsk.user.UserProfile profile();

    /** Returns who we serve and in what order. */
    dev.slsk.spi.UploadPolicy uploadPolicy();

    /** Returns what admits or refuses a peer's request. */
    dev.slsk.internal.UploadAdmission admission();

    /**
     * Returns what we tell peers our average upload speed is, in bytes per
     * second.
     *
     * <p>The server's average for this account as last heard, not a local
     * estimate. It goes into every search response, where peers read it to
     * rank us as a source.
     */
    int advertisedUploadSpeed();

    /**
     * Serves a file to a peer whose request the policy allowed.
     *
     * @param user who asked
     * @param path the file they asked for
     */
    void serve(dev.slsk.user.Username user, String path);

    /** What an unsolicited offer of a file turned out to be. */
    enum OfferDisposition {
        TAKEN,
        COMPLETE,
        UNKNOWN
    }

    /**
     * Answers a peer's unsolicited offer of a file.
     *
     * @param username who offered
     * @param filename what they offered
     * @param offer the request itself
     * @return what the offer turned out to be
     */
    OfferDisposition offered(
            String username, String filename, dev.slsk.internal.messaging.messages.TransferRequest offer);

    /**
     * Records where a peer says one of our downloads is in its queue.
     *
     * <p>Called for every place-in-queue a peer sends, not only the ones we
     * asked for. A peer volunteering one — this library's own uploader answers a
     * {@code QueueUpload} with one — used to be dropped, because the only thing
     * reading these was the wait registered by whoever had asked.
     *
     * @param username who sent it
     * @param filename the file it is about
     * @param position where that peer says we are
     */
    void queuePosition(String username, String filename, int position);
}
