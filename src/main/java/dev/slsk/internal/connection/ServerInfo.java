// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.connection;

/** Session information from the server. */
public record ServerInfo(
        Integer parentMinSpeed, Integer parentSpeedRatio, Integer wishlistInterval, Boolean supporter) {

    public ServerInfo() {
        this(null, null, null, null);
    }

    public ServerInfo(Integer parentMinSpeed) {
        this(parentMinSpeed, null, null, null);
    }

    public ServerInfo(Integer parentMinSpeed, Integer parentSpeedRatio) {
        this(parentMinSpeed, parentSpeedRatio, null, null);
    }

    public ServerInfo(Integer parentMinSpeed, Integer parentSpeedRatio, Integer wishlistInterval) {
        this(parentMinSpeed, parentSpeedRatio, wishlistInterval, null);
    }

    public ServerInfo with(
            Integer replacementParentMinSpeed,
            Integer replacementParentSpeedRatio,
            Integer replacementWishlistInterval,
            Boolean replacementSupporter) {
        return new ServerInfo(
                replacementParentMinSpeed != null ? replacementParentMinSpeed : parentMinSpeed,
                replacementParentSpeedRatio != null ? replacementParentSpeedRatio : parentSpeedRatio,
                replacementWishlistInterval != null ? replacementWishlistInterval : wishlistInterval,
                replacementSupporter != null ? replacementSupporter : supporter);
    }
}
