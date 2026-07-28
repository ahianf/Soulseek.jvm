// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.events.ShareEvent;
import java.util.List;

/**
 * What we offer to the network.
 *
 * <p>Announcing the counts to the server is part of a successful scan rather
 * than a separate call a consumer has to remember. Forgetting it was easy on the
 * old surface and the consequence is invisible: the share is served correctly
 * and the server keeps telling everyone we have nothing, which is exactly the
 * signal peers use to refuse us.
 *
 * <p>Replacing the built-in index with an application's own catalogue arrives in
 * a later phase.
 */
public interface Shares {

    /**
     * Sets the folders to share, replacing whatever was set before.
     *
     * <p>Does not scan; call {@link #rescan}.
     *
     * @param folders the folders
     */
    void configure(List<SharedFolder> folders);

    /**
     * Returns the folders currently configured.
     *
     * @return the folders
     */
    List<SharedFolder> configured();

    /**
     * Rebuilds the index and announces the resulting counts to the server.
     *
     * <p>Blocks. On a large share this takes a while, which is why it reports
     * progress through {@link #events()}.
     *
     * @param signal cancels the scan
     * @return the index as rebuilt
     */
    ShareIndex rescan(CancellationSignal signal);

    /**
     * Returns what we are currently sharing.
     *
     * @return the index
     */
    ShareIndex index();

    /**
     * Returns the stream of share events.
     *
     * @return the event stream
     */
    EventStream<ShareEvent> events();
}
