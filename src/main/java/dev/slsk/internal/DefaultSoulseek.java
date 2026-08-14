// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.Chat;
import dev.slsk.Connection;
import dev.slsk.Diagnostics;
import dev.slsk.Downloads;
import dev.slsk.Me;
import dev.slsk.Rooms;
import dev.slsk.Search;
import dev.slsk.Shares;
import dev.slsk.Soulseek;
import dev.slsk.Uploads;
import dev.slsk.Users;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.events.EventBus;
import dev.slsk.internal.options.SoulseekClientOptions;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link Soulseek}, and the only way to get one.
 *
 * <p>The client interface the facets were bound to during Phase 3 is gone; what
 * is left underneath is the engine — connection lifecycle, component wiring,
 * and the seam the collaborators delegate through. The fold moves each blocking
 * wrapper body out of that engine and into the facet that owns it, one facet at
 * a time, until nothing is left to move.
 */
public final class DefaultSoulseek implements Soulseek {

    private final SoulseekEngine client;
    private final DefaultConnection connection;
    private final DefaultChat chat;
    private final DefaultMe me;
    private final DefaultUsers users;
    private final DefaultDiagnostics diagnostics;
    private final DefaultRooms rooms;
    private final DefaultSearch search;
    private final DefaultDownloads downloads;
    private final DefaultUploads uploads;
    private final DefaultShares shares;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Every bus this client owns, so {@link #close()} can stop every delivery
     * thread. One per facet, plus the mesh bus the diagnostics facet holds
     * alongside its own.
     */
    private final java.util.List<EventBus<?>> buses = new java.util.ArrayList<>();

    private DefaultSoulseek(
            SoulseekEngine client, DefaultConnection.Credentials credentials, dev.slsk.spi.TransferStore store) {
        this.client = Objects.requireNonNull(client, "client");
        DiagnosticSink diagnostics = client.getDiagnostic();
        this.connection = new DefaultConnection(client, credentials, bus("connection", diagnostics));
        this.chat = new DefaultChat(client, bus("chat", diagnostics), diagnostics);
        this.me = new DefaultMe(
                client, dev.slsk.user.Username.of(credentials.username()), bus("me", diagnostics), diagnostics);
        this.users = new DefaultUsers(client, bus("users", diagnostics), diagnostics);
        this.diagnostics = new DefaultDiagnostics(client, bus("diagnostics", diagnostics), bus("mesh", diagnostics));
        this.rooms = new DefaultRooms(client, bus("rooms", diagnostics));
        this.search = new DefaultSearch(client, bus("search", diagnostics));
        this.downloads = new DefaultDownloads(client, bus("downloads", diagnostics), store);
        this.uploads = new DefaultUploads(client, bus("uploads", diagnostics));
        this.shares = new DefaultShares(client, bus("shares", diagnostics));
        // Metrics counts transfers, and the facets that hold them are built
        // after the one that reports on them.
        this.diagnostics.bind(downloads, uploads);
    }

    /** Creates a bus and records it, so close() can stop its delivery thread. */
    private <T> EventBus<T> bus(String name, DiagnosticSink diagnostics) {
        EventBus<T> created = new EventBus<>(name, diagnostics);
        buses.add(created);
        return created;
    }

    /**
     * Creates a client.
     *
     * @param username the account to log in as
     * @param password the account password
     * @param minorVersion the application minor version, which the server
     *     requires and which must be unique per client
     * @param options the client options
     * @return the client
     */
    public static Soulseek create(String username, String password, int minorVersion, SoulseekClientOptions options) {
        return create(username, password, minorVersion, options, dev.slsk.spi.TransferStore.inMemory());
    }

    /**
     * Creates a client whose download queue is recorded somewhere.
     *
     * @param username the account to log in as
     * @param password the account password
     * @param minorVersion the application minor version
     * @param options the client options
     * @param store where the download queue is recorded
     * @return the client
     */
    public static Soulseek create(
            String username,
            String password,
            int minorVersion,
            SoulseekClientOptions options,
            dev.slsk.spi.TransferStore store) {
        SoulseekEngine client = new SoulseekEngine(minorVersion, options);
        return new DefaultSoulseek(client, new DefaultConnection.Credentials(username, password), store);
    }

    /**
     * Wraps an engine the caller has already configured.
     *
     * <p>{@link #create} builds its own, which is right for a consumer and
     * useless for a test that has to drive a probe connection or a stubbed
     * connection manager. Package-private, because nothing outside this package
     * can get hold of an engine to pass one.
     *
     * @param client the engine to bind the facets to
     * @param username the account the facets should report as ours
     * @param password the account password
     * @return the client
     */
    static Soulseek over(SoulseekEngine client, String username, String password) {
        return new DefaultSoulseek(
                client, new DefaultConnection.Credentials(username, password), dev.slsk.spi.TransferStore.inMemory());
    }

    @Override
    public Connection connection() {
        return connection;
    }

    @Override
    public Chat chat() {
        return chat;
    }

    @Override
    public Me me() {
        return me;
    }

    @Override
    public Users users() {
        return users;
    }

    @Override
    public Diagnostics diagnostics() {
        return diagnostics;
    }

    @Override
    public Rooms rooms() {
        return rooms;
    }

    @Override
    public Search search() {
        return search;
    }

    @Override
    public Downloads downloads() {
        return downloads;
    }

    @Override
    public Uploads uploads() {
        return uploads;
    }

    @Override
    public Shares shares() {
        return shares;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // Reconnects first of all: closing the engine disconnects, and a
            // supervisor still listening would read that as a drop and start
            // retrying a client that is being disposed.
            connection.close();
            // The queue next: a transfer cancelled by a closing socket looks
            // like a peer failure and would be retried on the way down.
            downloads.close();
            search.close();
            client.close();
            // Last: a facet closing above may still publish a terminal event,
            // and a bus stopped before that would drop it.
            buses.forEach(EventBus::close);
        }
    }

    /** The client the facets are still bound to. Removed by the fold. */
    SoulseekEngine client() {
        return client;
    }
}
