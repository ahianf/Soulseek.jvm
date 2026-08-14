// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.user.Username;
import java.util.Objects;

/**
 * One file on one peer: the key for anything held per user and per path.
 *
 * <p>The maps this keys used to be keyed by the two joined with a delimiter,
 * and the delimiter had to be a byte a Soulseek path cannot contain — which in
 * practice meant a raw NUL, written literally into the Java source. That made
 * two source files binary to git: {@code grep}, {@code diff}, review and every
 * static-analysis tool degraded on them, and the only thing gained was avoiding
 * a two-field record.
 *
 * @param user whose file it is
 * @param path the remote path, backslash-joined as the network writes it
 */
record PeerFile(Username user, String path) {

    PeerFile {
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(path, "path");
    }
}
