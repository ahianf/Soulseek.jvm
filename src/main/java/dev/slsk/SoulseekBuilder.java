// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.download.DownloadPolicy;
import dev.slsk.share.SharedFolder;
import dev.slsk.spi.ShareCatalog;
import dev.slsk.spi.TransferStore;
import dev.slsk.spi.UploadPolicy;
import dev.slsk.user.UserProfile;
import java.nio.file.Path;
import java.time.Duration;
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
    private final List<SharedFolder> shares = new ArrayList<>();
    private DownloadPolicy downloads = DownloadPolicy.defaults();
    private UploadPolicy uploads = UploadPolicy.standard(2, 1);
    private TransferStore transferStore = TransferStore.inMemory();
    private ShareCatalog catalog;
    private UserProfile profile = UserProfile.empty();
    private Duration peerTimeout = Duration.ofSeconds(60);
    private Duration transferTimeout = Duration.ofSeconds(60);
    private Duration messageTimeout = Duration.ofSeconds(10);

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
     * How long a peer connection may sit idle before it is dropped.
     *
     * <p>Load-bearing, and not only as a connection setting: the same budget
     * bounds the wait for a peer's transfer acknowledgement, and that wait is
     * registered before the request is written. On a congested write queue a
     * short budget can be spent before the request even leaves, and the download
     * then reports a timeout the peer was never asked about.
     *
     * @param value the idle budget
     * @return this builder
     */
    public SoulseekBuilder peerTimeout(Duration value) {
        this.peerTimeout = positive(value, "peerTimeout");
        return this;
    }

    /**
     * How long an established transfer may move no bytes before it is dropped.
     *
     * <p>The default is generous on purpose. A congested uploader splitting its
     * line several ways can stall for a long time and still be working; the
     * reference clients put no such timer on an active transfer at all.
     *
     * @param value the stall budget
     * @return this builder
     */
    public SoulseekBuilder transferTimeout(Duration value) {
        this.transferTimeout = positive(value, "transferTimeout");
        return this;
    }

    /**
     * How long to wait for a server response before giving up on it.
     *
     * <p>Bounds a peer-address lookup among other things. A server under load
     * can miss a short budget, and losing that race used to kill the download
     * that was waiting on it.
     *
     * @param value the response budget
     * @return this builder
     */
    public SoulseekBuilder messageTimeout(Duration value) {
        this.messageTimeout = positive(value, "messageTimeout");
        return this;
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException(name + " must be positive: " + value);
        }
        return value;
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
                List.copyOf(shares),
                downloads,
                uploads,
                transferStore,
                catalog,
                profile,
                peerTimeout,
                transferTimeout,
                messageTimeout);
    }
}
