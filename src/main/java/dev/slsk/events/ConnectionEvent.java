// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import dev.slsk.ConnectionState;
import dev.slsk.ServerInfo;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * What happened to the server connection.
 *
 * <p>Three separate registrations — connected, disconnected, state changed —
 * collapse into {@link StateChanged}, because they were three views of one
 * transition and a consumer that wanted all of them registered three listeners
 * and reassembled the order itself.
 */
public sealed interface ConnectionEvent extends SoulseekEvent {

    /**
     * The connection moved from one state to another.
     *
     * @param from the previous state
     * @param to the new state
     * @param at when
     */
    record StateChanged(ConnectionState from, ConnectionState to, Instant at) implements ConnectionEvent {
        public StateChanged {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(to, "to");
            Objects.requireNonNull(at, "at");
        }
    }

    /**
     * The server told us something about itself.
     *
     * @param info everything it has said so far, not only the new part
     * @param at when
     */
    record ServerInfoReceived(ServerInfo info, Instant at) implements ConnectionEvent {
        public ServerInfoReceived {
            Objects.requireNonNull(info, "info");
            Objects.requireNonNull(at, "at");
        }
    }

    /**
     * The server kicked us off, usually because the same account logged in
     * elsewhere.
     *
     * @param reason what it said
     * @param at when
     */
    record KickedFromServer(String reason, Instant at) implements ConnectionEvent {
        public KickedFromServer {
            Objects.requireNonNull(reason, "reason");
            Objects.requireNonNull(at, "at");
        }
    }

    /**
     * An announcement broadcast to every user on the server.
     *
     * @param message the announcement
     * @param at when
     */
    record GlobalMessageReceived(String message, Instant at) implements ConnectionEvent {
        public GlobalMessageReceived {
            Objects.requireNonNull(message, "message");
            Objects.requireNonNull(at, "at");
        }
    }

    /**
     * Terms the server will not accept in a search.
     *
     * @param phrases the excluded phrases
     * @param at when
     */
    record ExcludedSearchPhrasesReceived(List<String> phrases, Instant at) implements ConnectionEvent {
        public ExcludedSearchPhrasesReceived {
            phrases = List.copyOf(Objects.requireNonNull(phrases, "phrases"));
            Objects.requireNonNull(at, "at");
        }
    }
}
