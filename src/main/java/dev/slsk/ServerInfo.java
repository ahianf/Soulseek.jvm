// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * What the server told us about itself when we logged in.
 *
 * <p>Every field is optional because the server sends these as separate
 * messages, at its own pace, and a client that has just logged in may have
 * received some, all, or none of them. The type it replaces used boxed {@code
 * Integer} and {@code Boolean} to mean the same thing, which put the burden of
 * remembering that on the caller and produced a {@code NullPointerException}
 * when they forgot.
 *
 * @param parentMinSpeed the minimum speed the server requires of a distributed
 *     parent
 * @param parentSpeedRatio the ratio the server uses to compute how many
 *     distributed children we should accept
 * @param wishlistInterval how often the server permits a wishlist search
 * @param supporter whether the server considers this account a supporter
 */
public record ServerInfo(
        OptionalInt parentMinSpeed,
        OptionalInt parentSpeedRatio,
        Optional<Duration> wishlistInterval,
        Optional<Boolean> supporter) {

    private static final ServerInfo EMPTY =
            new ServerInfo(OptionalInt.empty(), OptionalInt.empty(), Optional.empty(), Optional.empty());

    /** Validates and returns the info. */
    public ServerInfo {
        java.util.Objects.requireNonNull(parentMinSpeed, "parentMinSpeed");
        java.util.Objects.requireNonNull(parentSpeedRatio, "parentSpeedRatio");
        java.util.Objects.requireNonNull(wishlistInterval, "wishlistInterval");
        java.util.Objects.requireNonNull(supporter, "supporter");
    }

    /**
     * Returns the state before the server has said anything.
     *
     * @return info with every field absent
     */
    public static ServerInfo empty() {
        return EMPTY;
    }

    /**
     * Returns a copy with the parent minimum speed set.
     *
     * @param value the speed
     * @return the updated info
     */
    public ServerInfo withParentMinSpeed(int value) {
        return new ServerInfo(OptionalInt.of(value), parentSpeedRatio, wishlistInterval, supporter);
    }

    /**
     * Returns a copy with the parent speed ratio set.
     *
     * @param value the ratio
     * @return the updated info
     */
    public ServerInfo withParentSpeedRatio(int value) {
        return new ServerInfo(parentMinSpeed, OptionalInt.of(value), wishlistInterval, supporter);
    }

    /**
     * Returns a copy with the wishlist interval set.
     *
     * @param value the interval
     * @return the updated info
     */
    public ServerInfo withWishlistInterval(Duration value) {
        return new ServerInfo(parentMinSpeed, parentSpeedRatio, Optional.of(value), supporter);
    }

    /**
     * Returns a copy with the supporter flag set.
     *
     * @param value whether the account is a supporter
     * @return the updated info
     */
    public ServerInfo withSupporter(boolean value) {
        return new ServerInfo(parentMinSpeed, parentSpeedRatio, wishlistInterval, Optional.of(value));
    }
}
