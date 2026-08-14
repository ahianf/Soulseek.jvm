// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.search.FileAttributes;
import dev.slsk.search.SearchFile;
import dev.slsk.share.BrowseResponse;
import dev.slsk.share.Directory;
import dev.slsk.share.RemotePath;
import dev.slsk.share.ShareIndex;
import dev.slsk.share.SharedFolder;
import dev.slsk.spi.ResolvedFile;
import dev.slsk.spi.ShareCatalog;
import dev.slsk.user.Username;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * What {@code Shares.rescan} produces: an in-memory catalog of the configured
 * folders.
 *
 * <p>Before this, configuring a share and scanning it announced counts to the
 * server and served nothing — a client that told every peer it had ten thousand
 * files and then answered every browse with an empty list, which is worse than
 * sharing nothing. The scan builds what it counted.
 *
 * <p>Deliberately simple, and deliberately not the last word. It holds every
 * path in memory and matches a search by substring, which is the right shape for
 * a few thousand files and the wrong one for a few hundred thousand. An
 * application with a real index implements {@link ShareCatalog} and installs it,
 * which is what the SPI is for.
 *
 * <p>Resolution goes through {@link RemotePath#toLocal}, so a peer-supplied path
 * is checked against the share root it claims rather than trusted. Every
 * rejection is the same empty result: distinguishing "no such file" from "you
 * may not have that" would make the reply a filesystem oracle.
 */
final class ScannedShareCatalog implements ShareCatalog {

    /** One configured folder, and the name it is advertised under. */
    private record Root(String name, Path path, boolean locked) {}

    private final List<Root> roots;
    private final Map<String, Directory> directories;
    private final List<Directory> open;
    private final List<Directory> locked;
    private final List<SearchFile> files;
    private final ShareIndex index;

    private ScannedShareCatalog(
            List<Root> roots,
            Map<String, Directory> directories,
            List<Directory> open,
            List<Directory> locked,
            List<SearchFile> files,
            ShareIndex index) {
        this.roots = roots;
        this.directories = directories;
        this.open = open;
        this.locked = locked;
        this.files = files;
        this.index = index;
    }

    /** Accumulates a scan, so the walk stays in {@code DefaultShares}. */
    static final class Builder {

        private final List<Root> roots = new ArrayList<>();
        private final Map<String, List<SearchFile>> byDirectory = new LinkedHashMap<>();
        private long bytes;

        /**
         * Registers a folder, returning the name it is advertised under.
         *
         * @param folder the configured folder
         * @return the share name, or empty if the folder cannot be advertised
         */
        Optional<String> root(SharedFolder folder) {
            Path path = folder.path().toAbsolutePath().normalize();
            Path name = path.getFileName();
            if (name == null || name.toString().isBlank()) {
                // A filesystem root has no name to advertise it under, and a
                // share called "" is not a share.
                return Optional.empty();
            }
            roots.add(new Root(name.toString(), path, folder.locked()));
            return Optional.of(name.toString());
        }

        /**
         * Records one file.
         *
         * @param shareName the share it belongs to
         * @param root the share's local root
         * @param file the local file
         * @param size its size in bytes
         */
        void file(String shareName, Path root, Path file, long size) {
            String remote;
            try {
                remote = RemotePath.toRemote(shareName, root, file);
            } catch (IllegalArgumentException outside) {
                // A symlink pointing out of the share, most likely. Not ours to
                // advertise.
                return;
            }
            byDirectory
                    .computeIfAbsent(RemotePath.parent(remote), key -> new ArrayList<>())
                    .add(new SearchFile(remote, size, FileAttributes.none()));
            bytes += size;
        }

        /**
         * Returns the catalog, and the index describing it.
         *
         * @param scannedAt when the scan finished
         * @return the catalog
         */
        ScannedShareCatalog build(java.time.Instant scannedAt) {
            Map<String, Directory> directories = new LinkedHashMap<>();
            List<Directory> open = new ArrayList<>();
            List<Directory> locked = new ArrayList<>();
            List<SearchFile> files = new ArrayList<>();
            for (Map.Entry<String, List<SearchFile>> entry : byDirectory.entrySet()) {
                Directory directory = new Directory(entry.getKey(), entry.getValue());
                directories.put(entry.getKey(), directory);
                files.addAll(entry.getValue());
                (isLocked(entry.getKey()) ? locked : open).add(directory);
            }
            ShareIndex index = new ShareIndex(
                    directories.size(), files.size(), bytes, Optional.of(scannedAt), ShareIndex.ScanStatus.READY);
            return new ScannedShareCatalog(
                    List.copyOf(roots),
                    Map.copyOf(directories),
                    List.copyOf(open),
                    List.copyOf(locked),
                    List.copyOf(files),
                    index);
        }

        private boolean isLocked(String remoteDirectory) {
            return roots.stream()
                    .filter(Root::locked)
                    .anyMatch(root ->
                            remoteDirectory.equals(root.name()) || remoteDirectory.startsWith(root.name() + "\\"));
        }
    }

    @Override
    public BrowseResponse browse(Username requester) {
        return new BrowseResponse(open, locked);
    }

    @Override
    public List<Directory> directory(Username requester, String path) {
        Directory directory = directories.get(path);
        return directory == null ? List.of() : List.of(directory);
    }

    @Override
    public List<SearchFile> search(Username requester, String terms, int limit) {
        if (terms == null || terms.isBlank() || limit <= 0) {
            return List.of();
        }
        String[] words = terms.toLowerCase(Locale.ROOT).split("\\s+");
        List<SearchFile> matches = new ArrayList<>();
        for (SearchFile file : files) {
            String candidate = file.path().toLowerCase(Locale.ROOT);
            boolean all = true;
            for (String word : words) {
                if (!word.isEmpty() && !candidate.contains(word)) {
                    all = false;
                    break;
                }
            }
            if (all) {
                matches.add(file);
                if (matches.size() >= limit) {
                    break;
                }
            }
        }
        return List.copyOf(matches);
    }

    @Override
    public Optional<ResolvedFile> resolve(Username requester, String path) {
        for (Root root : roots) {
            Optional<Path> local = RemotePath.toLocal(path, root.name(), root.path());
            if (local.isEmpty()) {
                continue;
            }
            try {
                return Optional.of(ResolvedFile.of(local.get()));
            } catch (IOException unreadable) {
                // Present in the index, gone or unreadable now. One answer for
                // every rejection.
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    @Override
    public ShareIndex index() {
        return index;
    }

    /** Returns the roots, so a caller can tell whether anything is configured. */
    boolean isEmpty() {
        return Objects.requireNonNull(files).isEmpty();
    }
}
