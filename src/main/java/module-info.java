// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

/**
 * Soulseek.jvm.
 *
 * <p>Mid-rewrite toward the 1.0 facet API. The implementation lives under
 * {@code dev.slsk.internal.*} and is not exported. See
 * {@code JAVA_API_1_0_GOAL.md}.
 *
 * <p>Four exports, with one rule each: {@code dev.slsk} is what you call,
 * {@code dev.slsk.events} is what you receive, {@code dev.slsk.exceptions}
 * is what can go wrong, and {@code dev.slsk.spi} is what you implement.
 * Everything else is internal, and a test asserts that none of it is reachable
 * from a signature here.
 */
module dev.slsk.soulseek {
    exports dev.slsk;
    exports dev.slsk.connection;
    exports dev.slsk.diagnostics;
    exports dev.slsk.events;
    exports dev.slsk.exceptions;
    exports dev.slsk.room;
    exports dev.slsk.search;
    exports dev.slsk.share;
    exports dev.slsk.spi;
    exports dev.slsk.user;
}
