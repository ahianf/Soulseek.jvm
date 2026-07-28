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
import dev.slsk.Soulseek;
import dev.slsk.Uploads;
import dev.slsk.Users;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.diagnostics.GlobalDiagnostic;
import dev.slsk.internal.options.SoulseekClientOptions;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link Soulseek} over the {@link SoulseekClient} seam.
 *
 * <p>This is the facade half of "facade, then fold". Each facet is bound to the
 * blocking client rather than to the future-shaped collaborators underneath it,
 * because the blocking wrappers that adapt those collaborators live in {@code
 * DefaultSoulseekClient} and moving them is a separate change from introducing
 * the facet that will own them. The fold moves each wrapper body down into its
 * facet and deletes the client interface; until then both exist, and only this
 * one is exported.
 */
public final class DefaultSoulseek implements Soulseek {

    private final SoulseekClient client;
    private final DefaultConnection connection;
    private final DefaultChat chat;
    private final DefaultMe me;
    private final DefaultUsers users;
    private final DefaultDiagnostics diagnostics;
    private final DefaultRooms rooms;
    private final DefaultSearch search;
    private final DefaultDownloads downloads;
    private final DefaultUploads uploads;
    private final AtomicBoolean closed = new AtomicBoolean();

    private DefaultSoulseek(SoulseekClient client, DefaultConnection.Credentials credentials) {
        this.client = Objects.requireNonNull(client, "client");
        DiagnosticSink diagnostics = diagnosticSink();
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
    }

    /**
     * Where a contained listener fault is reported.
     *
     * <p><strong>Temporary.</strong> This routes to the process-wide {@link
     * GlobalDiagnostic} rather than to the client's own diagnostic listeners,
     * because the per-client sink lives behind {@code ClientContext} and the
     * facets only hold the {@link SoulseekClient} interface. That is a real
     * shortcoming — two clients in one JVM report through the same channel, and
     * a per-client dispatch policy was fixed once already for exactly that
     * reason. The fold gives facets the context, and this goes with it.
     */
    private static DiagnosticSink diagnosticSink() {
        return new DiagnosticSink() {
            @Override
            public void trace(String message) {
                GlobalDiagnostic.trace(message);
            }

            @Override
            public void trace(String message, Throwable exception) {
                GlobalDiagnostic.trace(message, exception);
            }

            @Override
            public void debug(String message) {
                GlobalDiagnostic.debug(message);
            }

            @Override
            public void debug(String message, Throwable exception) {
                GlobalDiagnostic.debug(message, exception);
            }

            @Override
            public void info(String message) {
                GlobalDiagnostic.info(message);
            }

            @Override
            public void warning(String message) {
                GlobalDiagnostic.warning(message);
            }

            @Override
            public void warning(String message, Throwable exception) {
                GlobalDiagnostic.warning(message, exception);
            }
        };
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
        SoulseekClient client =
                options == null ? SoulseekClient.create(minorVersion) : SoulseekClient.create(minorVersion, options);
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
    public void close() {
        if (closed.compareAndSet(false, true)) {
            client.close();
        }
    }

    /** The client the facets are still bound to. Removed by the fold. */
    SoulseekClient client() {
        return client;
    }
}
