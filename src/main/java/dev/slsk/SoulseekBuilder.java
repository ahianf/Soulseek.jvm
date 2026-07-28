// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.spi.ShareCatalog;
import dev.slsk.spi.TransferStore;
import dev.slsk.spi.UploadPolicy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Builds a {@link Soulseek}.
 *
 * <p>Everything except the credentials and the application's minor version has
 * a working default, so the shortest correct client is three calls. Everything
 * that used to be a constructor overload or an options record with thirty
 * positional parameters is a named method here, which is the difference between
 * a client you can read back and one you have to count commas in.
 *
 * <p>The minor version is required and has no sensible default: the server uses
 * it to tell client builds apart, and every client that shipped with a borrowed
 * one made someone else's traffic look like its own.
 */
public final class SoulseekBuilder {

    private String username;
    private String password;
    private int applicationMinorVersion = -1;
    private int listenPort = 2234;
    private DiagnosticLevel diagnostics = DiagnosticLevel.INFO;
    private final List<SharedFolder> shares = new ArrayList<>();
    private DownloadPolicy downloads = DownloadPolicy.defaults();
    private UploadPolicy uploads = UploadPolicy.standard(2, 1);
    private TransferStore transferStore = TransferStore.inMemory();
    private ShareCatalog catalog;
    private UserProfile profile = UserProfile.empty();

    SoulseekBuilder() {}

    /**
     * Sets the account to log in as.
     *
     * @param user the username
     * @param secret the password
     * @return this builder
     */
    public SoulseekBuilder credentials(String user, String secret) {
        this.username = Objects.requireNonNull(user, "username");
        this.password = Objects.requireNonNull(secret, "password");
        return this;
    }

    /**
     * Sets the application's minor version, which the server requires and which
     * must be unique to this client build.
     *
     * @param value the minor version, which must be greater than 100
     * @return this builder
     */
    public SoulseekBuilder applicationMinorVersion(int value) {
        if (value <= 100) {
            throw new IllegalArgumentException("applicationMinorVersion must be greater than 100: " + value);
        }
        this.applicationMinorVersion = value;
        return this;
    }

    /**
     * Sets the port peers connect to us on.
     *
     * @param value the port
     * @return this builder
     */
    public SoulseekBuilder listenPort(int value) {
        if (value < 1024 || value > 65_535) {
            throw new IllegalArgumentException("listenPort must be between 1024 and 65535: " + value);
        }
        this.listenPort = value;
        return this;
    }

    /**
     * Sets the folders to share.
     *
     * @param folders what to share
     * @return this builder
     */
    public SoulseekBuilder shares(List<SharedFolder> folders) {
        shares.clear();
        shares.addAll(Objects.requireNonNull(folders, "folders"));
        return this;
    }

    /**
     * Shares one folder.
     *
     * @param folder the folder's local path
     * @return this builder
     */
    public SoulseekBuilder share(Path folder) {
        shares.add(SharedFolder.of(folder));
        return this;
    }

    /**
     * Sets how the download queue is run.
     *
     * @param policy the policy
     * @return this builder
     */
    public SoulseekBuilder downloads(DownloadPolicy policy) {
        this.downloads = Objects.requireNonNull(policy, "policy");
        return this;
    }

    /**
     * Sets who we serve and in what order.
     *
     * @param policy the policy
     * @return this builder
     */
    public SoulseekBuilder uploads(UploadPolicy policy) {
        this.uploads = Objects.requireNonNull(policy, "policy");
        return this;
    }

    /**
     * Sets where the download queue survives a restart.
     *
     * @param store the store
     * @return this builder
     */
    public SoulseekBuilder transferStore(TransferStore store) {
        this.transferStore = Objects.requireNonNull(store, "store");
        return this;
    }

    /**
     * Serves browses, searches and uploads from a catalog of your own.
     *
     * @param value the catalog
     * @return this builder
     */
    public SoulseekBuilder catalog(ShareCatalog value) {
        this.catalog = Objects.requireNonNull(value, "catalog");
        return this;
    }

    /**
     * Sets what other users see when they ask about this account.
     *
     * @param value the profile
     * @return this builder
     */
    public SoulseekBuilder profile(UserProfile value) {
        this.profile = Objects.requireNonNull(value, "profile");
        return this;
    }

    /**
     * Sets how much the library says about what it is doing.
     *
     * @param level the level
     * @return this builder
     */
    public SoulseekBuilder diagnostics(DiagnosticLevel level) {
        this.diagnostics = Objects.requireNonNull(level, "level");
        return this;
    }

    /**
     * Builds the client. It does not connect; {@code connection().connect(...)}
     * does that.
     *
     * @return the client
     */
    public Soulseek build() {
        if (username == null) {
            throw new IllegalStateException("credentials are required");
        }
        if (applicationMinorVersion <= 100) {
            throw new IllegalStateException("applicationMinorVersion is required and must be greater than 100");
        }
        return dev.slsk.internal.SoulseekFactory.create(
                username,
                password,
                applicationMinorVersion,
                listenPort,
                diagnostics,
                List.copyOf(shares),
                downloads,
                uploads,
                transferStore,
                catalog,
                profile);
    }
}
