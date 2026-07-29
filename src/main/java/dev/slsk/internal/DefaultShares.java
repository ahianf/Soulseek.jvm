// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.CancellationSignal;
import dev.slsk.EventStream;
import dev.slsk.ShareIndex;
import dev.slsk.SharedFolder;
import dev.slsk.Shares;
import dev.slsk.events.ShareEvent;
import dev.slsk.spi.ShareCatalog;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * {@link Shares}, over the engine.
 *
 * <p>Scanning walks the configured folders, builds a catalog of what it finds,
 * installs it as what peers are served from, and tells the server the counts.
 *
 * <p>The announcement is part of the scan rather than something the consumer
 * calls afterwards, because forgetting it is easy and the symptom is invisible
 * from the inside: the share is served perfectly well while the server keeps
 * telling every peer we have nothing, which is the exact signal most clients use
 * to decline to serve us.
 *
 * <p>Building the catalog is part of it for the mirror-image reason. A scan that
 * counted ten thousand files and served none of them announced a share that did
 * not exist, which is worse than announcing nothing.
 *
 * <p>{@link #catalog} replaces the scanned catalog outright. After that a rescan
 * still counts and still announces — the counts are the server's business either
 * way — but what a peer sees comes from the installed catalog.
 */
final class DefaultShares implements Shares {

    private final SoulseekEngine client;
    private final EventBus<ShareEvent> events;
    private final AtomicReference<List<SharedFolder>> folders = new AtomicReference<>(List.of());
    private final AtomicReference<ShareIndex> index = new AtomicReference<>(ShareIndex.empty());

    /** Set once a consumer installs its own catalog; a rescan stops replacing it. */
    private final AtomicBoolean installed = new AtomicBoolean();

    DefaultShares(SoulseekEngine client, EventBus<ShareEvent> events) {
        this.client = Objects.requireNonNull(client, "client");
        this.events = Objects.requireNonNull(events, "events");
        // The counts are per-session state on the server's side, so they have to
        // be re-sent on every login. Forgetting that is invisible from the
        // inside: the share is served perfectly well while the server tells
        // every peer we have nothing.
        client.events().on(EngineEvents.Kind.LOGGED_IN, (Void ignored) -> announce());
    }

    /**
     * Tells the server what we are sharing.
     *
     * <p>Reads the index rather than the scan, so an installed catalog announces
     * its own counts. Without this a consumer that replaced the built-in index
     * announced whatever the last scan of the configured folders found, which
     * for a consumer with its own catalog is nothing at all.
     */
    private void announce() {
        ShareIndex current = index();
        if (current.fileCount() == 0 && current.directoryCount() == 0) {
            return;
        }
        try {
            client.server().setSharedCounts(current.directoryCount(), current.fileCount());
        } catch (RuntimeException failure) {
            client.getDiagnostic().warning("Failed to announce the share counts", failure);
        }
    }

    @Override
    public void configure(List<SharedFolder> value) {
        folders.set(List.copyOf(Objects.requireNonNull(value, "folders")));
    }

    @Override
    public List<SharedFolder> configured() {
        return folders.get();
    }

    @Override
    public ShareIndex rescan(CancellationSignal signal) {
        Objects.requireNonNull(signal, "signal");
        events.publish(new ShareEvent.ScanStarted(Instant.now()));
        index.set(new ShareIndex(
                index.get().directoryCount(),
                index.get().fileCount(),
                index.get().totalBytes(),
                index.get().lastScan(),
                ShareIndex.ScanStatus.SCANNING));

        ScannedShareCatalog.Builder builder = new ScannedShareCatalog.Builder();
        int directories = 0;
        int files = 0;
        try {
            for (SharedFolder folder : folders.get()) {
                signal.throwIfCancellationRequested();
                if (!Files.isDirectory(folder.path())) {
                    continue;
                }
                Optional<String> shareName = builder.root(folder);
                if (shareName.isEmpty()) {
                    continue;
                }
                Path root = folder.path().toAbsolutePath().normalize();
                try (Stream<Path> walk = Files.walk(root)) {
                    for (Path entry : walk.toList()) {
                        signal.throwIfCancellationRequested();
                        if (Files.isDirectory(entry)) {
                            directories++;
                        } else if (Files.isRegularFile(entry)) {
                            files++;
                            builder.file(shareName.get(), root, entry, Files.size(entry));
                        }
                    }
                }
                events.publish(new ShareEvent.ScanProgressed(directories, files, Instant.now()));
            }
        } catch (IOException | RuntimeException failure) {
            ShareIndex failed = new ShareIndex(
                    index.get().directoryCount(),
                    index.get().fileCount(),
                    index.get().totalBytes(),
                    index.get().lastScan(),
                    ShareIndex.ScanStatus.FAILED);
            index.set(failed);
            throw failure instanceof RuntimeException runtime
                    ? runtime
                    : new IllegalStateException("the share scan failed", failure);
        }

        ScannedShareCatalog scannedCatalog = builder.build(Instant.now());
        if (!installed.get()) {
            client.setShareCatalog(scannedCatalog);
        }
        ShareIndex scanned = scannedCatalog.index();
        index.set(scanned);
        // The counts announced are the catalog's, not the walk's. They differ —
        // the walk sees empty directories and the catalog does not — and
        // announcing one while reporting the other would leave the server and
        // index() disagreeing about the same share.
        client.server().setSharedCounts(scanned.directoryCount(), scanned.fileCount());
        events.publish(new ShareEvent.ScanCompleted(scanned, Instant.now()));
        return scanned;
    }

    @Override
    public void catalog(ShareCatalog value) {
        Objects.requireNonNull(value, "catalog");
        installed.set(true);
        client.setShareCatalog(value);
        index.set(value.index());
        // Installing a catalog changes what we are sharing, which the server has
        // to be told. It is also how a consumer whose own index changed says so.
        announce();
    }

    @Override
    public ShareIndex index() {
        return installed.get() ? client.catalog().index() : index.get();
    }

    @Override
    public EventStream<ShareEvent> events() {
        return events;
    }
}
