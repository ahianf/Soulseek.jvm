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

    private final DefaultSoulseekClient client;
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

    private DefaultSoulseek(DefaultSoulseekClient client, DefaultConnection.Credentials credentials) {
        this.client = Objects.requireNonNull(client, "client");
        DiagnosticSink diagnostics = client.getDiagnostic();
        this.connection = new DefaultConnection(client, credentials, new EventBus<>("connection", diagnostics));
        this.chat = new DefaultChat(client, new EventBus<>("chat", diagnostics), diagnostics);
        this.me = new DefaultMe(
                client, dev.slsk.Username.of(credentials.username()), new EventBus<>("me", diagnostics), diagnostics);
        this.users = new DefaultUsers(client, new EventBus<>("users", diagnostics), diagnostics);
        this.diagnostics = new DefaultDiagnostics(
                client, new EventBus<>("diagnostics", diagnostics), new EventBus<>("mesh", diagnostics));
        this.rooms = new DefaultRooms(client, new EventBus<>("rooms", diagnostics));
        this.search = new DefaultSearch(client, new EventBus<>("search", diagnostics));
        this.downloads = new DefaultDownloads(client, new EventBus<>("downloads", diagnostics));
        this.uploads = new DefaultUploads(client, new EventBus<>("uploads", diagnostics));
        this.shares = new DefaultShares(client, new EventBus<>("shares", diagnostics));
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
        DefaultSoulseekClient client = new DefaultSoulseekClient(minorVersion, options);
        return new DefaultSoulseek(client, new DefaultConnection.Credentials(username, password));
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
    static Soulseek over(DefaultSoulseekClient client, String username, String password) {
        return new DefaultSoulseek(client, new DefaultConnection.Credentials(username, password));
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
            client.close();
        }
    }

    /** The client the facets are still bound to. Removed by the fold. */
    DefaultSoulseekClient client() {
        return client;
    }
}
