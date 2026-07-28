// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.events.ConnectionEvent;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * The connection to the Soulseek server.
 *
 * <p>One {@code Soulseek} client outlives every socket it opens. The consumer
 * builds it once for the process and never replaces it; connecting, dropping and
 * connecting again all happen underneath, and the watched users and shares
 * registered with the server are re-established on login rather than being the
 * consumer's job to remember.
 */
public interface Connection {

    /**
     * Connects to the default server and logs in with the configured
     * credentials. Blocks until online, or until it fails.
     *
     * @param signal cancels the attempt; {@link CancellationSignal#none()} to
     *     let it run to completion
     */
    void connect(CancellationSignal signal);

    /**
     * Connects to a named server and logs in. Blocks until online, or until it
     * fails.
     *
     * @param address where to connect
     * @param signal cancels the attempt
     */
    void connect(ServerAddress address, CancellationSignal signal);

    /**
     * Disconnects.
     *
     * <p>Idempotent: disconnecting an already-disconnected client does nothing
     * rather than throwing.
     *
     * @param reason recorded in diagnostics and reported to listeners
     */
    void disconnect(String reason);

    /**
     * Returns the current state.
     *
     * <p>Synchronous and cheap, so a consumer rendering cold never needs to have
     * been listening.
     *
     * @return what the connection is doing now
     */
    ConnectionState state();

    /**
     * Returns what the server has said about itself, if anything.
     *
     * @return the server info, or empty when not logged in
     */
    Optional<ServerInfo> server();

    /**
     * Measures the round trip to the server.
     *
     * @param signal cancels the measurement
     * @return the round-trip time
     */
    Duration ping(CancellationSignal signal);

    /**
     * Returns the stream of connection events.
     *
     * @return the event stream
     */
    EventStream<ConnectionEvent> events();

    /**
     * Takes the current state and subscribes, as one atomic step.
     *
     * @param listener receives every subsequent event
     * @return the state as it was, and the subscription
     */
    Attachment<ConnectionState> attach(Consumer<ConnectionEvent> listener);
}
