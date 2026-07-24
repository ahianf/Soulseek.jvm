// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

/**
 * Session information from the server.
 *
 * <p>Values are {@code null} until the client is connected and logged in.</p>
 */
public class ServerInfo {
    private final Boolean supporter;
    private final Integer parentMinSpeed;
    private final Integer parentSpeedRatio;
    private final Integer wishlistInterval;

    /**
     * Creates empty server information.
     */
    public ServerInfo() {
        this(null, null, null, null);
    }

    /**
     * Creates server information with a parent minimum speed.
     *
     * @param parentMinSpeed the parent minimum speed
     */
    public ServerInfo(Integer parentMinSpeed) {
        this(parentMinSpeed, null, null, null);
    }

    /**
     * Creates server information with parent speed values.
     *
     * @param parentMinSpeed the parent minimum speed
     * @param parentSpeedRatio the parent speed ratio
     */
    public ServerInfo(Integer parentMinSpeed, Integer parentSpeedRatio) {
        this(parentMinSpeed, parentSpeedRatio, null, null);
    }

    /**
     * Creates server information with parent speed and wishlist values.
     *
     * @param parentMinSpeed the parent minimum speed
     * @param parentSpeedRatio the parent speed ratio
     * @param wishlistInterval the wishlist interval in seconds
     */
    public ServerInfo(Integer parentMinSpeed, Integer parentSpeedRatio, Integer wishlistInterval) {
        this(parentMinSpeed, parentSpeedRatio, wishlistInterval, null);
    }

    /**
     * Creates server information.
     *
     * @param parentMinSpeed the parent minimum speed
     * @param parentSpeedRatio the parent speed ratio
     * @param wishlistInterval the wishlist interval in seconds
     * @param isSupporter whether the logged-in user has ever purchased privileges
     */
    public ServerInfo(Integer parentMinSpeed, Integer parentSpeedRatio, Integer wishlistInterval, Boolean isSupporter) {
        this.parentMinSpeed = parentMinSpeed;
        this.parentSpeedRatio = parentSpeedRatio;
        this.wishlistInterval = wishlistInterval;
        this.supporter = isSupporter;
    }

    /**
     * Returns whether the logged-in user has ever purchased privileges.
     *
     * @return the supporter status, or {@code null}
     */
    public final Boolean isSupporter() {
        return supporter;
    }

    /**
     * Returns the parent minimum speed.
     *
     * @return the parent minimum speed, or {@code null}
     */
    public final Integer getParentMinSpeed() {
        return parentMinSpeed;
    }

    /**
     * Returns the parent speed ratio.
     *
     * @return the parent speed ratio, or {@code null}
     */
    public final Integer getParentSpeedRatio() {
        return parentSpeedRatio;
    }

    /**
     * Returns the wishlist-search interval in seconds.
     *
     * @return the wishlist interval, or {@code null}
     */
    public final Integer getWishlistInterval() {
        return wishlistInterval;
    }

    ServerInfo with(
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
