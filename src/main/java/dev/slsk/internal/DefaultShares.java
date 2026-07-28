// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.CancellationSignal;
import dev.slsk.EventStream;
import dev.slsk.ShareIndex;
import dev.slsk.SharedFolder;
import dev.slsk.Shares;
import dev.slsk.events.ShareEvent;
import dev.slsk.internal.common.Blocking;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * {@link Shares}, over the engine.
 *
 * <p>Scanning walks the configured folders and counts what it finds, then tells
 * the server. The announcement is part of the scan rather than something the
 * consumer calls afterwards, because forgetting it is easy and the symptom is
 * invisible from the inside: the share is served perfectly well while the server
 * keeps telling every peer we have nothing, which is the exact signal most
 * clients use to decline to serve us.
 */
final class DefaultShares implements Shares {

    private final DefaultSoulseekClient client;
    private final EventBus<ShareEvent> events;
    private final AtomicReference<List<SharedFolder>> folders = new AtomicReference<>(List.of());
    private final AtomicReference<ShareIndex> index = new AtomicReference<>(ShareIndex.empty());

    DefaultShares(DefaultSoulseekClient client, EventBus<ShareEvent> events) {
        this.client = Objects.requireNonNull(client, "client");
        this.events = Objects.requireNonNull(events, "events");
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

        int directories = 0;
        int files = 0;
        long bytes = 0;
        try {
            for (SharedFolder folder : folders.get()) {
                signal.throwIfCancellationRequested();
                if (!Files.isDirectory(folder.path())) {
                    continue;
                }
                try (Stream<Path> walk = Files.walk(folder.path())) {
                    for (Path entry : walk.toList()) {
                        signal.throwIfCancellationRequested();
                        if (Files.isDirectory(entry)) {
                            directories++;
                        } else if (Files.isRegularFile(entry)) {
                            files++;
                            bytes += Files.size(entry);
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

        ShareIndex scanned =
                new ShareIndex(directories, files, bytes, Optional.of(Instant.now()), ShareIndex.ScanStatus.READY);
        index.set(scanned);
        Blocking.await(client.server().setSharedCounts(directories, files));
        events.publish(new ShareEvent.ScanCompleted(scanned, Instant.now()));
        return scanned;
    }

    @Override
    public ShareIndex index() {
        return index.get();
    }

    @Override
    public EventStream<ShareEvent> events() {
        return events;
    }
}
