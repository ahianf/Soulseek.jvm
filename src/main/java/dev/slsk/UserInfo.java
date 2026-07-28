// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.Objects;
import java.util.Optional;

/**
 * A user's self-description, as they chose to present it.
 *
 * <p>Everything here is supplied by the peer, so none of it is trustworthy and
 * the picture in particular is arbitrary bytes from a stranger. Treat it as
 * display data.
 *
 * @param user who this describes
 * @param description their free-text description
 * @param picture their avatar, if they set one
 * @param uploadSlots how many concurrent uploads they permit
 * @param queueLength how many transfers are waiting in their queue
 * @param hasFreeUploadSlot whether they have capacity right now
 */
public record UserInfo(
        Username user,
        String description,
        Optional<byte[]> picture,
        int uploadSlots,
        int queueLength,
        boolean hasFreeUploadSlot) {

    /** Validates and returns the info. */
    public UserInfo {
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(description, "description");
        Objects.requireNonNull(picture, "picture");
    }
}
