// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

/**
 * Soulseek.jvm.
 *
 * <p>The public surface is organized by responsibility. {@code dev.slsk}
 * contains the client entry point, facets, cancellation, and subscriptions;
 * capability packages contain their immutable requests and snapshots;
 * {@code events} contains notifications, {@code exceptions} contains failures,
 * and {@code spi} contains extension points.
 *
 * <p>The implementation lives under {@code dev.slsk.internal.*} and is not
 * exported. A test asserts that none of it is reachable from a public
 * signature.
 */
module dev.slsk.soulseek {
    exports dev.slsk;
    exports dev.slsk.connection;
    exports dev.slsk.diagnostics;
    exports dev.slsk.download;
    exports dev.slsk.events;
    exports dev.slsk.exceptions;
    exports dev.slsk.room;
    exports dev.slsk.search;
    exports dev.slsk.share;
    exports dev.slsk.spi;
    exports dev.slsk.transfer;
    exports dev.slsk.upload;
    exports dev.slsk.user;
}
