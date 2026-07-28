// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.time.Instant;
import java.util.Objects;

/**
 * Where the connection to the server stands.
 *
 * <p>This replaces a bit-flag set — {@code CONNECTED | LOGGED_IN} meant online,
 * and a caller had to know that — with states that name themselves and carry
 * their own data.
 *
 * <p>{@link Rejected} is deliberately not a kind of {@link Reconnecting}. A
 * wrong password does not become right by waiting, and a client that retries it
 * forever is both a bug and, from the server's side, abuse. Making the two
 * structurally different means a consumer cannot write a retry loop that treats
 * them the same without the compiler pointing at it.
 */
public sealed interface ConnectionState {

    /** Not connected, and not trying to be. */
    record Offline() implements ConnectionState {}

    /**
     * Opening the socket.
     *
     * @param attempt which attempt this is, counting from one
     */
    record Connecting(int attempt) implements ConnectionState {
        public Connecting {
            if (attempt < 1) {
                throw new IllegalArgumentException("attempt counts from one: " + attempt);
            }
        }
    }

    /** Socket open, logging in. */
    record Authenticating() implements ConnectionState {}

    /**
     * Connected and logged in.
     *
     * @param since when the session began
     * @param server what the server has told us about itself so far
     */
    record Online(Instant since, ServerInfo server) implements ConnectionState {
        public Online {
            Objects.requireNonNull(since, "since");
            Objects.requireNonNull(server, "server");
        }
    }

    /** Closing the connection at our own request. */
    record Disconnecting() implements ConnectionState {}

    /**
     * Waiting to try again after a failure.
     *
     * <p><strong>Not produced in 1.0.</strong> Automatic reconnection is 1.1
     * work; this variant ships now so that adding it then is additive rather
     * than a change to a sealed hierarchy every consumer switches over. A 1.0
     * consumer must still handle it for the switch to compile, which is the
     * point.
     *
     * @param attempt which attempt the next one will be
     * @param nextAttemptAt when it will happen, so a UI can count down and offer
     *     a working retry button rather than an indefinite spinner
     * @param lastFailure why the last attempt failed
     */
    record Reconnecting(int attempt, Instant nextAttemptAt, Throwable lastFailure) implements ConnectionState {
        public Reconnecting {
            Objects.requireNonNull(nextAttemptAt, "nextAttemptAt");
            Objects.requireNonNull(lastFailure, "lastFailure");
        }
    }

    /**
     * Terminal. The server refused the credentials, and retrying will not help.
     *
     * @param reason what the server said
     */
    record Rejected(String reason) implements ConnectionState {
        public Rejected {
            Objects.requireNonNull(reason, "reason");
        }
    }

    /**
     * Returns whether the client is connected and logged in.
     *
     * @return {@code true} when {@link Online}
     */
    default boolean isOnline() {
        return this instanceof Online;
    }
}
