// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.events.UploadEvent;
import dev.slsk.spi.UploadPolicy;
import dev.slsk.transfer.Priority;
import dev.slsk.transfer.TransferId;
import dev.slsk.user.Username;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Uploads peers have asked us for.
 *
 * <p>There is no {@code start} here, because we do not start uploads — peers ask
 * and a policy answers. What is ours is cancelling, reordering our own queue,
 * and deciding who we serve.
 *
 * <p>Cancelling an upload needs the queue that arrives in a later phase and is
 * not declared here yet.
 *
 * <p>{@link #ban} lives here rather than on {@code users()} for that last
 * reason. It names a user, but what it changes is our upload policy, and a
 * method belongs to the facet owning the state it mutates.
 */
public interface Uploads {

    /**
     * Returns every upload the library is holding.
     *
     * @return the uploads
     */
    List<Upload> all();

    /**
     * Returns an upload.
     *
     * @param id the upload
     * @return its current state
     * @throws IllegalArgumentException if it is not known
     */
    Upload get(TransferId id);

    /**
     * Returns an upload if it is known.
     *
     * @param id the upload
     * @return its current state, or empty
     */
    Optional<Upload> find(TransferId id);

    /**
     * Stops an upload.
     *
     * <p>An idempotent intent: cancelling an upload that already finished is a
     * request that has been overtaken, not an error. The peer is told, so their
     * client stops waiting rather than timing out.
     *
     * @param id which upload
     */
    void cancel(TransferId id);

    /**
     * Moves an upload within our own queue.
     *
     * <p>This orders work we have not started yet, so it applies to an upload
     * still queued and does nothing to one already running — that slot is
     * already taken. Within a priority, peers are served round-robin, and a
     * user the server marks privileged outranks every priority: that part is
     * protocol-mandated rather than a matter of taste.
     *
     * <p>This is also how an application expresses a favoured peer. The library
     * has no buddy list and should not grow one, because who counts as a buddy
     * is the application's knowledge; raising their queued uploads to
     * {@link Priority#HIGH} is how that preference reaches the queue.
     *
     * @param id the upload
     * @param priority its new priority
     */
    void prioritize(TransferId id, Priority priority);

    /**
     * Refuses to serve a user from now on. Idempotent.
     *
     * @param user who to refuse
     * @param reason recorded, and sent to them when they ask
     */
    /**
     * Returns who we serve and in what order.
     *
     * @return the policy
     */
    UploadPolicy policy();

    /**
     * Changes who we serve and in what order.
     *
     * @param policy the policy
     */
    void policy(UploadPolicy policy);

    void ban(Username user, String reason);

    /**
     * Serves a user again. Idempotent, and unbanning somebody who is not banned
     * does nothing.
     *
     * @param user who to unban
     */
    void unban(Username user);

    /**
     * Returns who we are refusing, and why.
     *
     * @return the bans
     */
    Map<Username, String> banned();

    /**
     * Returns the stream of upload events.
     *
     * @return the event stream
     */
    EventStream<UploadEvent> events();

    /**
     * Takes the current uploads and subscribes, as one atomic step.
     *
     * @param listener receives every subsequent event
     * @return the uploads as they were, and the subscription
     */
    Attachment<List<Upload>> attach(Consumer<UploadEvent> listener);
}
