// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.spi;

import dev.slsk.user.Username;
import java.util.Objects;

/**
 * A peer asking for one of our files.
 *
 * @param user who is asking
 * @param path the file they want, in Soulseek's backslash-joined form
 * @param size the file's size in bytes, or {@code 0} if it is not known yet
 */
public record UploadRequest(Username user, String path, long size) {

    /** Validates and returns the request. */
    public UploadRequest {
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(path, "path");
        if (size < 0) {
            throw new IllegalArgumentException("size must not be negative: " + size);
        }
    }
}
