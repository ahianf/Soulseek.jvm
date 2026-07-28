// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.events.DownloadEvent;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Downloads, and the queue that runs them.
 *
 * <p>Every command is an idempotent intent taking an id: {@code pause(id)},
 * never {@code toggle(id)}. Pausing an already-paused download does nothing.
 * Cancelling a finished one does nothing. None of them throws for being asked
 * twice, because the consumer is usually an HTTP handler that cannot know
 * whether its previous request arrived.
 *
 * <p><strong>The managed queue is not built yet</strong>, so {@code pause},
 * {@code resume}, {@code retry} and {@code await} are not declared here; they
 * arrive with it. Declaring a method before it works only moves the discovery
 * of that fact from the developer wiring it up to the user wondering why the
 * button does nothing. Everything declared here works.
 */
public interface Downloads {

    /**
     * Enqueues a download.
     *
     * @param request what to fetch
     * @return the id of this enqueue
     */
    TransferId enqueue(DownloadRequest request);

    /**
     * Enqueues several downloads.
     *
     * @param requests what to fetch
     * @return the ids, in the order given
     */
    List<TransferId> enqueueAll(List<DownloadRequest> requests);

    /**
     * Stops a download. Idempotent, and does nothing to a finished one.
     *
     * @param id the download
     */
    void cancel(TransferId id);

    /**
     * Drops a finished download from the list.
     *
     * @param id the download
     * @throws IllegalStateException if it has not finished
     */
    void forget(TransferId id);

    /**
     * Moves a download within our own queue. Says nothing to the peer, and
     * cannot affect the peer's queue.
     *
     * @param id the download
     * @param priority its new priority
     */
    void prioritize(TransferId id, Priority priority);

    /**
     * Returns a download.
     *
     * @param id the download
     * @return its current state
     * @throws IllegalArgumentException if it is not known
     */
    Download get(TransferId id);

    /**
     * Returns a download if it is known.
     *
     * @param id the download
     * @return its current state, or empty
     */
    Optional<Download> find(TransferId id);

    /**
     * Returns every download the library is holding.
     *
     * @return the downloads
     */
    List<Download> all();

    /**
     * Returns the stream of download events.
     *
     * @return the event stream
     */
    EventStream<DownloadEvent> events();

    /**
     * Takes the current downloads and subscribes, as one atomic step.
     *
     * @param listener receives every subsequent event
     * @return the downloads as they were, and the subscription
     */
    Attachment<List<Download>> attach(Consumer<DownloadEvent> listener);
}
