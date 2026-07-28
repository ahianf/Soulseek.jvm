// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import dev.slsk.SearchId;
import dev.slsk.SearchResponse;
import dev.slsk.SearchStatus;
import dev.slsk.Username;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Search activity, both ours and other people's.
 *
 * <p>The first two are about searches we ran. The last three are about searches
 * <em>other</em> peers ran that we matched and answered, which is the other half
 * of being on a distributed network and which a client has to serve whether or
 * not it is searching itself.
 */
public sealed interface SearchEvent extends SoulseekEvent {

    /**
     * Peers answered one of our searches.
     *
     * <p>Carries a list rather than one response per event. The list is usually
     * of size one today, and it is a list so that batching responses to a
     * configured cadence later changes no type and breaks no consumer.
     *
     * @param id which search
     * @param responses the responses, already filtered
     * @param revision the snapshot revision these produced
     * @param at when
     */
    record ResponsesReceived(SearchId id, List<SearchResponse> responses, long revision, Instant at)
            implements SearchEvent {
        public ResponsesReceived {
            Objects.requireNonNull(id, "id");
            responses = List.copyOf(Objects.requireNonNull(responses, "responses"));
            Objects.requireNonNull(at, "at");
        }
    }

    /**
     * A search of ours started or stopped.
     *
     * @param id which search
     * @param from the previous status
     * @param to the current status
     * @param at when
     */
    record StatusChanged(SearchId id, SearchStatus from, SearchStatus to, Instant at) implements SearchEvent {
        public StatusChanged {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(to, "to");
            Objects.requireNonNull(at, "at");
        }
    }

    /**
     * Another peer searched, and it reached us.
     *
     * @param user who searched
     * @param terms what they searched for
     * @param token their token for it
     * @param at when
     */
    record RequestReceived(Username user, String terms, int token, Instant at) implements SearchEvent {
        public RequestReceived {
            Objects.requireNonNull(user, "user");
            Objects.requireNonNull(terms, "terms");
            Objects.requireNonNull(at, "at");
        }
    }

    /**
     * We answered another peer's search.
     *
     * @param user who we answered
     * @param token their token
     * @param fileCount how many files we offered
     * @param at when
     */
    record ResponseDelivered(Username user, int token, int fileCount, Instant at) implements SearchEvent {
        public ResponseDelivered {
            Objects.requireNonNull(user, "user");
            Objects.requireNonNull(at, "at");
        }
    }

    /**
     * We tried to answer another peer's search and could not reach them.
     *
     * @param user who we could not reach
     * @param token their token
     * @param cause what went wrong
     * @param at when
     */
    record ResponseDeliveryFailed(Username user, int token, Throwable cause, Instant at) implements SearchEvent {
        public ResponseDeliveryFailed {
            Objects.requireNonNull(user, "user");
            Objects.requireNonNull(cause, "cause");
            Objects.requireNonNull(at, "at");
        }
    }
}
