// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.transfer;

/**
 * A transfer rate limit.
 *
 * <p>A bare {@code long} reads wrong at every call site: {@code speedLimit(0)}
 * is ambiguous between "stopped" and "unlimited", and {@code speedLimit(1000)}
 * does not say whether it means bits, bytes, or kilobytes. Naming the unit in
 * the factory removes the guess, and {@link #unlimited()} says what zero means.
 *
 * <p>Megabits are offered alongside bytes because that is the unit an internet
 * connection is sold in, and a user setting a cap is usually thinking in terms
 * of the line rather than the file.
 *
 * @param bytesPerSecond the limit in bytes per second; {@code 0} means unlimited
 */
public record Bandwidth(long bytesPerSecond) {

    private static final Bandwidth UNLIMITED = new Bandwidth(0);

    private static final double BITS_PER_BYTE = 8d;
    private static final double MEGABIT = 1_000_000d;

    /**
     * Validates and returns the limit.
     *
     * @throws IllegalArgumentException if {@code bytesPerSecond} is negative
     */
    public Bandwidth {
        if (bytesPerSecond < 0) {
            throw new IllegalArgumentException("bandwidth must not be negative: " + bytesPerSecond);
        }
    }

    /**
     * Returns the unlimited rate.
     *
     * @return a bandwidth that imposes no cap
     */
    public static Bandwidth unlimited() {
        return UNLIMITED;
    }

    /**
     * Returns a limit in bytes per second.
     *
     * @param bytesPerSecond the limit; {@code 0} means unlimited
     * @return the limit
     */
    public static Bandwidth ofBytesPerSecond(long bytesPerSecond) {
        return bytesPerSecond == 0 ? UNLIMITED : new Bandwidth(bytesPerSecond);
    }

    /**
     * Returns a limit in kibibytes per second, the unit most clients present.
     *
     * @param kibibytesPerSecond the limit
     * @return the limit
     */
    public static Bandwidth ofKibibytesPerSecond(double kibibytesPerSecond) {
        return ofBytesPerSecond(Math.round(kibibytesPerSecond * 1024));
    }

    /**
     * Returns a limit in megabits per second, the unit a connection is sold in.
     *
     * @param megabitsPerSecond the limit
     * @return the limit
     */
    public static Bandwidth ofMegabitsPerSecond(double megabitsPerSecond) {
        return ofBytesPerSecond(Math.round(megabitsPerSecond * MEGABIT / BITS_PER_BYTE));
    }

    /**
     * Returns whether this imposes no cap.
     *
     * @return {@code true} if unlimited
     */
    public boolean isUnlimited() {
        return bytesPerSecond == 0;
    }

    @Override
    public String toString() {
        return isUnlimited() ? "unlimited" : bytesPerSecond + " B/s";
    }
}
