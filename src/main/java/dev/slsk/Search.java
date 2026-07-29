// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.events.SearchEvent;
import java.util.List;
import java.util.function.Consumer;

/**
 * Searching the network.
 *
 * <p>Three ways to run one, because there are three genuinely different things a
 * caller wants. {@link #run} blocks and hands back everything, which is what a
 * script wants. {@link #start} returns an id immediately and lets results arrive
 * as events, which is what a UI wants. {@link #await} bridges the two for a
 * caller that started a search and later decided to wait for it.
 *
 * <p>A search that finds nothing returns an empty list. It is not an error, and
 * on this network it is not even unusual.
 *
 * <p>Nothing here groups, deduplicates, ranks or sorts. Those are presentation
 * decisions that every application makes differently, and a library that picked
 * one would be wrong for the others and awkward to override.
 *
 * <p><strong>Finished searches are kept, but not forever.</strong> A snapshot
 * holds every response its search received, so the hundred most recently
 * finished are retained and older ones are dropped; {@link #get} and {@link
 * #await} then no longer know them. Running searches are never dropped. An
 * application that wants a longer history keeps the {@link SearchResult} it was
 * handed, which is immutable and complete.
 */
public interface Search {

    /**
     * Starts a search and returns immediately.
     *
     * @param query what to search for
     * @return the search id
     */
    SearchId start(SearchQuery query);

    /**
     * Waits for a running search to stop.
     *
     * @param id the search
     * @param signal stops waiting, and stops the search
     * @return the finished search
     */
    SearchResult await(SearchId id, CancellationSignal signal);

    /**
     * Starts a search and waits for it. The common case.
     *
     * @param query what to search for
     * @param signal stops the search
     * @return the finished search
     */
    SearchResult run(SearchQuery query, CancellationSignal signal);

    /**
     * Stops a search early. Idempotent, and stopping a finished search does
     * nothing.
     *
     * @param id the search
     */
    void stop(SearchId id);

    /**
     * Returns a search as it stands.
     *
     * @param id the search
     * @return the snapshot
     * @throws IllegalArgumentException if the search is not known
     */
    SearchSnapshot get(SearchId id);

    /**
     * Returns every search still running.
     *
     * @return the running searches
     */
    List<SearchSnapshot> active();

    /**
     * Returns the stream of search events.
     *
     * @return the event stream
     */
    EventStream<SearchEvent> events();

    /**
     * Takes the running searches and subscribes, as one atomic step.
     *
     * @param listener receives every subsequent event
     * @return the searches as they were, and the subscription
     */
    Attachment<List<SearchSnapshot>> attach(Consumer<SearchEvent> listener);
}
