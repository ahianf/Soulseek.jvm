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
 * <p><strong>Enqueueing does not start anything.</strong> The request goes into
 * the library's queue, which decides when it runs and against which peer, how
 * many run at once overall and per peer, and when a failed attempt is worth
 * retrying. {@link #policy(DownloadPolicy)} is where those rules are set.
 * Everything declared here works: a method that does not yet do its job is not
 * declared at all, because declaring it only moves the discovery of that fact
 * from the developer wiring it up to the user wondering why the button does
 * nothing.
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
    /**
     * Stops a download and leaves it queued.
     *
     * <p>An idempotent intent: pausing an already-paused download does nothing,
     * and pausing a finished one is a request that has been overtaken rather
     * than an error.
     *
     * @param id which download
     */
    void pause(TransferId id);

    /**
     * Puts a paused download back in the queue.
     *
     * @param id which download
     */
    void resume(TransferId id);

    /**
     * Puts a finished download back in the queue, its attempt count reset.
     *
     * <p>A no-op on a download that has not finished.
     *
     * @param id which download
     */
    void retry(TransferId id);

    /**
     * Blocks until a download reaches a terminal state.
     *
     * @param id which download
     * @param signal stops waiting; it does not cancel the download
     * @return the download as it finished
     */
    Download await(TransferId id, CancellationSignal signal);

    /**
     * Returns how the queue is being run.
     *
     * @return the policy
     */
    DownloadPolicy policy();

    /**
     * Changes how the queue is run, with immediate effect.
     *
     * @param policy the policy
     */
    void policy(DownloadPolicy policy);

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
