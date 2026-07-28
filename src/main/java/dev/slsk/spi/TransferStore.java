// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.spi;

import dev.slsk.Download;
import dev.slsk.TransferId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Where the download queue survives a restart.
 *
 * <p>The library owns the queue, which means it owns the problem of a queue that
 * outlives the process. This is the one decision it cannot make for you: whether
 * a hundred queued downloads should still be there tomorrow, and in what.
 *
 * <p>The default keeps them in memory, which is the correct answer for a
 * short-lived process and the wrong one for a daemon. Implementing this is how a
 * daemon says so.
 */
public interface TransferStore {

    /**
     * Records a download's current state, replacing any previous record of it.
     *
     * @param download the download to record
     */
    void save(Download download);

    /**
     * Forgets a download.
     *
     * @param id the download to forget
     */
    void delete(TransferId id);

    /**
     * Returns every recorded download, for restoring the queue at startup.
     *
     * @return the recorded downloads
     */
    List<Download> loadAll();

    /**
     * A store that keeps the queue in memory and loses it on exit.
     *
     * @return the default store
     */
    static TransferStore inMemory() {
        return new TransferStore() {
            private final Map<TransferId, Download> records = new LinkedHashMap<>();

            @Override
            public synchronized void save(Download download) {
                records.put(Objects.requireNonNull(download, "download").id(), download);
            }

            @Override
            public synchronized void delete(TransferId id) {
                records.remove(Objects.requireNonNull(id, "id"));
            }

            @Override
            public synchronized List<Download> loadAll() {
                return List.copyOf(new ArrayList<>(records.values()));
            }
        };
    }
}
