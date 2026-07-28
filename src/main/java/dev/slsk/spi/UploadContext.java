// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.spi;

import dev.slsk.UserStatistics;
import dev.slsk.Username;

/**
 * What is true when a peer asks for a file.
 *
 * <p>Everything a policy could reasonably want to decide on, gathered so that
 * deciding is a pure function of a request and a context. That is what makes an
 * upload policy testable: a decision is a value derived from two values, not
 * something you have to stand up a client to observe.
 */
public interface UploadContext {

    /**
     * Returns what the server says about the requester.
     *
     * @return their sharing figures
     */
    UserStatistics requesterStatistics();

    /**
     * Returns whether the requester has bought privileges.
     *
     * <p>Privileged users jump the queue. That is protocol-mandated rather than
     * a matter of taste, which is why {@link UploadPolicy#standard} applies it
     * and an application does not have to know about it.
     *
     * @return whether they are privileged
     */
    boolean requesterIsPrivileged();

    /**
     * Returns how much has already been sent to a user, this session.
     *
     * @param user who
     * @return the byte count
     */
    long bytesAlreadySentTo(Username user);

    /**
     * Returns how many uploads are running right now.
     *
     * @return the active slot count
     */
    int activeSlots();

    /**
     * Returns how many uploads are waiting.
     *
     * @return the queue depth
     */
    int queueDepth();

    /**
     * Returns how many uploads are running for the requester.
     *
     * @return the requester's active slot count
     */
    int activeSlotsForRequester();
}
