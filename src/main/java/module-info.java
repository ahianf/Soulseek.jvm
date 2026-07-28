// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

/**
 * Soulseek.jvm.
 *
 * <p>Mid-rewrite toward the 1.0 facet API. The implementation now lives under
 * {@code dev.slsk.internal.*} and is not exported; the public surface is
 * being rebuilt package by package, and every type earns its way back onto it.
 * See {@code JAVA_API_1_0_GOAL.md}.
 *
 * <p>The 1.0 target is exactly four exports: {@code dev.slsk} (what you
 * call), {@code dev.slsk.events} (what you receive), {@code
 * dev.slsk.exceptions} (what can go wrong), and {@code dev.slsk.spi}
 * (what you implement).
 */
module dev.slsk.soulseek {
    exports dev.slsk;
    exports dev.slsk.events;
    exports dev.slsk.exceptions;
}
