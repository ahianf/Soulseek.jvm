// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

/**
 * Translation between local files and the backslash-joined paths Soulseek puts
 * on the wire.
 *
 * <p>A shared file is advertised under a <em>virtual path</em>: the share's
 * name, then the file's location relative to that share's root, joined with
 * {@code \}. The peer never learns the local directory layout, and a share can
 * be moved on disk without changing what the network sees. This matches the
 * convention Nicotine+ uses, so a path built here round-trips through the
 * clients most peers are running.
 *
 * <p>The two directions are deliberately asymmetric:
 *
 * <ul>
 *   <li>{@link #toRemote(String, Path, Path)} takes inputs the caller controls.
 *       Bad input there is a programming error and throws.
 *   <li>{@link #toLocal(String, String, Path)} takes a path <em>a remote peer
 *       chose</em>. Every rejection is an empty {@link Optional}, never an
 *       exception and never a partial result, because the caller's next step is
 *       to open the file it names.
 * </ul>
 *
 * <p>{@code toLocal} is the boundary between the network and the local
 * filesystem. It rejects traversal segments, absolute and drive-rooted paths, a
 * leading segment that is not the share, and — as the backstop that catches
 * what syntax cannot — anything whose real path does not resolve inside the
 * real share root. That last check is what stops a symlink inside the share
 * from pointing out of it. A caller must still confirm the file is one it
 * actually shares; containment is necessary, not sufficient.
 *
 * <p>The splitting helpers ({@link #basename(String)}, {@link #parent(String)},
 * {@link #lastFolderSegment(String)}) interpret paths that already exist and
 * accept either separator, since remote clients send both. They are pure string
 * operations and never touch the filesystem.
 */
public final class RemotePath {

    /** The separator Soulseek puts on the wire. */
    private static final char SEPARATOR = '\\';

    /**
     * Stands in for a backslash inside a single local file or folder name.
     *
     * <p>A backslash is a legal character in a POSIX file name and a path
     * separator on the wire, so a file literally named {@code AC\DC.mp3} would
     * otherwise advertise a folder that does not exist. Nicotine+ solves this
     * with exactly this sentinel; using the same one means a Nicotine+ peer
     * reverses our escaping the way we intended.
     */
    private static final String BACKSLASH_SENTINEL = "@@BACKSLASH@@";

    private RemotePath() {}

    /**
     * Returns the final segment of a remote path — the file name.
     *
     * @param remotePath the remote path, either separator
     * @return the final segment, or the whole input when it cannot be split
     */
    public static String basename(String remotePath) {
        if (remotePath == null || remotePath.isEmpty()) {
            return "";
        }
        int cut = lastSeparator(remotePath);
        return cut >= 0 && cut < remotePath.length() - 1 ? remotePath.substring(cut + 1) : remotePath;
    }

    /**
     * Returns everything before the final segment — the parent folder path.
     *
     * @param remotePath the remote path, either separator
     * @return the parent path, or an empty string when there is none
     */
    public static String parent(String remotePath) {
        if (remotePath == null || remotePath.isEmpty()) {
            return "";
        }
        int cut = lastSeparator(remotePath);
        return cut > 0 ? remotePath.substring(0, cut) : "";
    }

    /**
     * Returns the deepest folder name — the final segment of the parent path.
     *
     * @param remotePath the remote path, either separator
     * @return the deepest folder name, or an empty string when there is none
     */
    public static String lastFolderSegment(String remotePath) {
        return basename(parent(remotePath));
    }

    /**
     * Builds the virtual path under which a local file is advertised.
     *
     * <p>{@code toRemote("Music", /srv/share, /srv/share/Album/track.flac)}
     * produces {@code Music\Album\track.flac}.
     *
     * @param shareName the share's name, which becomes the leading segment
     * @param root the share's local root directory
     * @param file the local file, which must lie under {@code root}
     * @return the virtual path, joined with {@code \}
     * @throws NullPointerException if any argument is {@code null}
     * @throws IllegalArgumentException if the share name is blank or contains a
     *     separator, or if the file is the root itself or lies outside it
     */
    public static String toRemote(String shareName, Path root, Path file) {
        Objects.requireNonNull(shareName, "shareName");
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(file, "file");
        if (shareName.isBlank()) {
            throw new IllegalArgumentException("shareName must not be blank");
        }
        if (containsSeparator(shareName)) {
            throw new IllegalArgumentException("shareName must not contain a path separator: " + shareName);
        }

        Path base = root.toAbsolutePath().normalize();
        Path target = file.toAbsolutePath().normalize();
        Path relative;
        try {
            relative = base.relativize(target);
        } catch (IllegalArgumentException failure) {
            // Different roots entirely — a separate drive on Windows, say.
            throw new IllegalArgumentException("file does not lie under root: " + file, failure);
        }
        if (relative.getNameCount() == 0 || relative.toString().isEmpty()) {
            throw new IllegalArgumentException("file is the share root itself: " + file);
        }

        StringBuilder remote = new StringBuilder(shareName);
        for (Path segment : relative) {
            String name = segment.toString();
            if (name.equals("..")) {
                throw new IllegalArgumentException("file does not lie under root: " + file);
            }
            remote.append(SEPARATOR).append(name.replace("\\", BACKSLASH_SENTINEL));
        }
        return remote.toString();
    }

    /**
     * Resolves a peer-supplied virtual path to a local file under one share
     * root, or rejects it.
     *
     * <p>The result, when present, is the file's real path: symbolic links are
     * resolved and the outcome is known to lie inside the real {@code root}.
     * An empty result means the path was malformed, named a different share,
     * attempted traversal, escaped the root, or does not exist. Callers must
     * not distinguish between those cases when answering the peer — one
     * message for every rejection, or the reply becomes a filesystem oracle.
     *
     * @param remotePath the path exactly as the peer sent it
     * @param shareName the name of the share being resolved against
     * @param root the share's local root directory
     * @return the resolved real path, or empty if the path is not acceptable
     */
    public static Optional<Path> toLocal(String remotePath, String shareName, Path root) {
        if (remotePath == null || shareName == null || root == null) {
            return Optional.empty();
        }
        if (remotePath.isBlank() || shareName.isBlank() || containsSeparator(shareName)) {
            return Optional.empty();
        }
        // A NUL byte truncates the name inside some native filesystem calls, so
        // what gets opened stops matching what was checked.
        if (remotePath.indexOf('\0') >= 0) {
            return Optional.empty();
        }

        // Separators are not collapsed. A doubled separator yields an empty
        // segment and is rejected below: nothing this class advertises contains
        // one, so an inbound path that does is not a path we served.
        String[] segments = remotePath.split("[\\\\/]", -1);
        // A leading empty segment means the path started with a separator: an
        // absolute POSIX path, or a UNC share.
        if (segments.length < 2 || !segments[0].equals(shareName)) {
            return Optional.empty();
        }

        Path base;
        try {
            base = root.toRealPath();
        } catch (IOException failure) {
            return Optional.empty();
        }

        Path resolved = base;
        for (int index = 1; index < segments.length; index++) {
            String segment = segments[index].replace(BACKSLASH_SENTINEL, "\\");
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                return Optional.empty();
            }
            // Reversing the escape reintroduces a backslash, which is an
            // ordinary name character on POSIX and a separator on Windows.
            // (java.io.File stays qualified: this package declares its own
            // File, and importing the other one would shadow it.)
            if (segment.indexOf('/') >= 0 || segment.indexOf(java.io.File.separatorChar) >= 0) {
                return Optional.empty();
            }
            try {
                resolved = resolved.resolve(segment);
            } catch (InvalidPathException failure) {
                return Optional.empty();
            }
        }

        Path real;
        try {
            real = resolved.toRealPath();
        } catch (IOException failure) {
            return Optional.empty();
        }
        // The backstop: syntax cannot rule out a symlink inside the share that
        // points outside it, but comparing real paths can.
        if (!real.startsWith(base)) {
            return Optional.empty();
        }
        return Optional.of(real);
    }

    private static int lastSeparator(String path) {
        return Math.max(path.lastIndexOf('\\'), path.lastIndexOf('/'));
    }

    private static boolean containsSeparator(String value) {
        return value.indexOf('\\') >= 0 || value.indexOf('/') >= 0;
    }
}
