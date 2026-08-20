// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.connection;

/** Session information from the server. */
public record ServerSessionInfo(
        Integer parentMinSpeed, Integer parentSpeedRatio, Integer wishlistInterval, Boolean supporter) {

    public ServerSessionInfo() {
        this(null, null, null, null);
    }

    public ServerSessionInfo(Integer parentMinSpeed) {
        this(parentMinSpeed, null, null, null);
    }

    public ServerSessionInfo(Integer parentMinSpeed, Integer parentSpeedRatio) {
        this(parentMinSpeed, parentSpeedRatio, null, null);
    }

    public ServerSessionInfo(Integer parentMinSpeed, Integer parentSpeedRatio, Integer wishlistInterval) {
        this(parentMinSpeed, parentSpeedRatio, wishlistInterval, null);
    }

    public ServerSessionInfo with(
            Integer replacementParentMinSpeed,
            Integer replacementParentSpeedRatio,
            Integer replacementWishlistInterval,
            Boolean replacementSupporter) {
        return new ServerSessionInfo(
                replacementParentMinSpeed != null ? replacementParentMinSpeed : parentMinSpeed,
                replacementParentSpeedRatio != null ? replacementParentSpeedRatio : parentSpeedRatio,
                replacementWishlistInterval != null ? replacementWishlistInterval : wishlistInterval,
                replacementSupporter != null ? replacementSupporter : supporter);
    }
}
